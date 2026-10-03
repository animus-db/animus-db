//! The per-format **transcode table** and disk-level transcode entry point
//! (ADR 0073, "The upgrade-restart harness").
//!
//! The harness runs a workload at the *current* format versions on `SimEnv`
//! disks, stops or crashes a node, **transcodes** each durable whole-file
//! format back to an older version (`target_back` versions behind current),
//! restarts on the current code, and verifies every acknowledged write.
//!
//! `lsm-wal`, `control-wal` and `shared-wal` are v2 and transcode to v1 for
//! real; every other format is still v1 only, so its only supported target
//! is the current version and its transcode is the identity. A request for
//! any unlisted target is an **error** ([`TranscodeError::UnsupportedTarget`]), never a
//! silent identity: a harness that asked for "one version back" and got the
//! bytes unchanged would be green while proving nothing.
//!
//! # Adding a format version (checklist step 7)
//!
//! Formats come in two harness classes, and the registration differs.
//!
//! * **Whole-file formats** (a file on a node's disk that one format owns:
//!   `lsm-wal`, `lsm-manifest`, `lsm-sstable`, `control-wal`, `shared-wal`,
//!   `encryption-envelope`) have a [`TABLE`] entry. A bump edits that entry in
//!   place: bump `current_version`, append a [`VersionSpec`] (with the
//!   capability mask of what that version can express), and point `transcode`
//!   at a function that decodes current bytes and re-encodes them with
//!   `legacy::vK` for each supported `K`.
//! * **Embedded formats** (records that live *inside* another format's file,
//!   or off a node's disk entirely) are listed in [`EMBEDDED`] with the
//!   [`Carrier`] that holds them. They get no `TABLE` entry of their own. A
//!   bump edits **the carrier's** transcode, so the carrier re-encodes every
//!   embedded record through the embedded format's legacy encoder (an
//!   embedded `Metadata` v2 -> v1 is part of `control-wal`'s transcode). Update
//!   the `EMBEDDED` row's `current_version` too. A [`Carrier::OffDisk`] format
//!   (segment, backup, config, CR, wire) is never transcoded by the disk pass;
//!   its upgrade coverage is its own per-version fixture test.
//!
//! **Legacy encoders the harness calls must be `pub`**, gated
//! `#[cfg(any(test, feature = "legacy-encoders"))]` (this crate enables the
//! feature on every format crate). A `cfg(test)`-only or crate-private encoder
//! is invisible to `animus-test`, so the transcode could not call it. Prefer a
//! type-erased byte-level function (old framing in, new framing out) over one
//! generic over the crate's record types.
//!
//! `tests/upgrade_restart_tier0.rs` is the backstop: the table's
//! `current_version` must equal the newest checked-in fixture, every fixture
//! version needs a `VersionSpec`, and every
//! `tests/fixtures/formats/<dir>` in the workspace must be named by `TABLE` or
//! `EMBEDDED` (with every carrier naming a real `TABLE` entry). Forgetting step
//! 7 is a red test, not a silent gap.

use std::fmt;
use std::io;
use std::sync::OnceLock;

use animus_env::Disk;
use animus_sim::SimEnv;
use futures::executor::block_on;

use crate::corpus::name_seed;

/// Placeholder capability mask per format version (ADR 0073: "a per-version
/// capability mask recorded next to the legacy encoder"). An older version can
/// only carry what it could express, so a workload targeting version N draws
/// only from the features whose bits are set here. No format has any
/// version-gated feature yet, so every mask is [`CapabilityMask::ALL`]; the
/// meaning of individual bits is assigned per format when its first bump lands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CapabilityMask(pub u64);

impl CapabilityMask {
    /// Every feature a workload could use (the only mask today).
    pub const ALL: Self = Self(u64::MAX);

    /// Whether every feature bit in `other` is available at this version.
    #[must_use]
    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

/// One supported version of a format and what it can express.
#[derive(Clone, Copy, Debug)]
pub struct VersionSpec {
    /// The format version (the tag's version number).
    pub version: u32,
    /// Workload features expressible at this version.
    pub capabilities: CapabilityMask,
}

/// A transcode function: bytes of the whole file at the entry's *current*
/// version in, bytes of the same logical file at `target` out. Called only
/// after [`FormatEntry::transcode_to`] has checked `target` is listed.
pub type TranscodeFn = fn(&FormatEntry, &[u8], u32) -> Result<Vec<u8>, TranscodeError>;

/// One durable whole-file format in the table.
pub struct FormatEntry {
    /// Stable slug, equal to the format's fixture directory name under
    /// `tests/fixtures/formats/`.
    pub name: &'static str,
    /// The version the current code writes.
    pub current_version: u32,
    /// Every version a transcode may target, ascending, ending with
    /// `current_version`.
    pub versions: &'static [VersionSpec],
    /// Current-version bytes to target-version bytes.
    pub transcode: TranscodeFn,
}

impl FormatEntry {
    /// The capability mask of `version`, if this entry supports it.
    #[must_use]
    pub fn capabilities(&self, version: u32) -> Option<CapabilityMask> {
        self.versions
            .iter()
            .find(|v| v.version == version)
            .map(|v| v.capabilities)
    }

    /// Transcode `bytes` (at `current_version`) to `target`.
    ///
    /// # Errors
    /// [`TranscodeError::UnsupportedTarget`] if `target` is not a listed
    /// version, plus whatever the entry's function reports.
    pub fn transcode_to(&self, bytes: &[u8], target: u32) -> Result<Vec<u8>, TranscodeError> {
        if self.capabilities(target).is_none() {
            return Err(self.unsupported(target));
        }
        (self.transcode)(self, bytes, target)
    }

    fn unsupported(&self, target: u32) -> TranscodeError {
        TranscodeError::UnsupportedTarget {
            format: self.name,
            target,
            supported: self.versions.iter().map(|v| v.version).collect(),
        }
    }
}

/// Why a transcode or a disk pass failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranscodeError {
    /// The requested target version is not one this format can be transcoded
    /// to. Never downgraded to an identity.
    UnsupportedTarget {
        /// Format slug.
        format: &'static str,
        /// The version asked for.
        target: u32,
        /// The versions the table does support for this format.
        supported: Vec<u32>,
    },
    /// The input bytes are not valid at the entry's current version.
    Malformed {
        /// Format slug.
        format: &'static str,
        /// What was wrong.
        detail: String,
    },
    /// A disk operation failed.
    Io(String),
}

impl fmt::Display for TranscodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedTarget {
                format,
                target,
                supported,
            } => write!(
                f,
                "{format}: cannot transcode to version {target} (supported: {supported:?})"
            ),
            Self::Malformed { format, detail } => write!(f, "{format}: malformed input: {detail}"),
            Self::Io(e) => write!(f, "io: {e}"),
        }
    }
}

impl std::error::Error for TranscodeError {}

impl From<TranscodeError> for io::Error {
    fn from(e: TranscodeError) -> Self {
        io::Error::new(io::ErrorKind::InvalidInput, e.to_string())
    }
}

/// Identity transcode for a format that has exactly one version: valid only
/// for the current version (and [`FormatEntry::transcode_to`] has already
/// refused every other target, so this re-checks defensively).
fn identity_current_only(
    entry: &FormatEntry,
    bytes: &[u8],
    target: u32,
) -> Result<Vec<u8>, TranscodeError> {
    if target != entry.current_version {
        return Err(entry.unsupported(target));
    }
    Ok(bytes.to_vec())
}

/// `CWL1`/`SWL1` transcode: current (v2) is the identity; v1 goes through
/// `animus-control`'s `legacy-encoders`-gated `reframe_to_v1`, which drops the
/// sync-marker lines and re-frames each record under the v1 tag.
fn line_wal_transcode(
    tag: &animus_control::format::FormatTag,
    entry: &FormatEntry,
    bytes: &[u8],
    target: u32,
) -> Result<Vec<u8>, TranscodeError> {
    match target {
        t if t == entry.current_version => Ok(bytes.to_vec()),
        1 => animus_control::format::reframe_to_v1(tag, bytes).map_err(|e| {
            TranscodeError::Malformed {
                format: entry.name,
                detail: e.to_string(),
            }
        }),
        t => Err(entry.unsupported(t)),
    }
}

fn control_wal_transcode(
    entry: &FormatEntry,
    bytes: &[u8],
    target: u32,
) -> Result<Vec<u8>, TranscodeError> {
    line_wal_transcode(&animus_control::persist::CONTROL_WAL, entry, bytes, target)
}

fn shared_wal_transcode(
    entry: &FormatEntry,
    bytes: &[u8],
    target: u32,
) -> Result<Vec<u8>, TranscodeError> {
    line_wal_transcode(
        &animus_control::persist::SHARED_WAL_TAG,
        entry,
        bytes,
        target,
    )
}

/// `LWL1` (LSM WAL segment) v2 -> v1: decode and re-encode the records under
/// the v1 header with no sync markers (`animus_storage::reframe_wal_to_v1`).
fn lsm_wal_transcode(
    entry: &FormatEntry,
    bytes: &[u8],
    target: u32,
) -> Result<Vec<u8>, TranscodeError> {
    match target {
        t if t == entry.current_version => Ok(bytes.to_vec()),
        1 => animus_storage::reframe_wal_to_v1(bytes).map_err(|e| TranscodeError::Malformed {
            format: entry.name,
            detail: e.to_string(),
        }),
        t => Err(entry.unsupported(t)),
    }
}

const V1_ONLY: &[VersionSpec] = &[VersionSpec {
    version: 1,
    capabilities: CapabilityMask::ALL,
}];

/// `CWL1`/`SWL1`/`LWL1` v1 (no sync markers) and v2 (sync markers, issue #1132). A
/// v2 file transcodes to v1 by dropping the marker lines and re-framing each
/// record under the v1 tag (`animus_control::format::reframe_to_v1`).
const V1_V2: &[VersionSpec] = &[
    VersionSpec {
        version: 1,
        capabilities: CapabilityMask::ALL,
    },
    VersionSpec {
        version: 2,
        capabilities: CapabilityMask::ALL,
    },
];

/// The table: one entry per durable whole-file format ADR 0073 inventories
/// that lives on a node's disk. Names equal the fixture directory names.
pub static TABLE: &[FormatEntry] = &[
    FormatEntry {
        name: "lsm-wal",
        current_version: 2,
        versions: V1_V2,
        transcode: lsm_wal_transcode,
    },
    FormatEntry {
        name: "lsm-manifest",
        current_version: 1,
        versions: V1_ONLY,
        transcode: identity_current_only,
    },
    FormatEntry {
        name: "lsm-sstable",
        current_version: 1,
        versions: V1_ONLY,
        transcode: identity_current_only,
    },
    FormatEntry {
        name: "control-wal",
        current_version: 2,
        versions: V1_V2,
        transcode: control_wal_transcode,
    },
    FormatEntry {
        name: "shared-wal",
        current_version: 2,
        versions: V1_V2,
        transcode: shared_wal_transcode,
    },
    // The `ADE1` encryption envelope wraps whole files of an `EncryptedEnv`
    // disk; transcoding the files *inside* it needs the key, so a real
    // transcode here arrives with the first envelope bump. Identity today.
    FormatEntry {
        name: "encryption-envelope",
        current_version: 1,
        versions: V1_ONLY,
        transcode: identity_current_only,
    },
];

/// Where an embedded format's bytes live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Carrier {
    /// Inside the whole-file format named by this [`TABLE`] entry; a bump of
    /// the embedded format is implemented by that entry's transcode.
    Table(&'static str),
    /// Not on a node's disk (segment/backup objects, config, CR, wire). The
    /// disk pass never touches it.
    OffDisk,
}

/// A versioned format ADR 0073 puts behind dispatch that is **not** a whole
/// node-disk file, so it has no [`TABLE`] entry (see the module header).
#[derive(Clone, Copy, Debug)]
pub struct Embedded {
    /// Stable slug, equal to its fixture directory name under the owning
    /// crate's `tests/fixtures/formats/`.
    pub name: &'static str,
    /// What holds it.
    pub carrier: Carrier,
    /// The crate that owns the format and its fixtures.
    pub owner: &'static str,
    /// The version the current code writes (`1` for the untagged, frozen
    /// formats and pinned vectors, which are v1 by definition).
    pub current_version: u32,
}

const fn emb(name: &'static str, carrier: Carrier, owner: &'static str) -> Embedded {
    emb_v(name, carrier, owner, 1)
}

/// [`emb`] for an embedded format already past v1.
const fn emb_v(
    name: &'static str,
    carrier: Carrier,
    owner: &'static str,
    current_version: u32,
) -> Embedded {
    Embedded {
        name,
        carrier,
        owner,
        current_version,
    }
}

/// Every versioned format that is not a whole node-disk file. Together with
/// [`TABLE`] this must name every `tests/fixtures/formats/<dir>` in the
/// workspace (enforced by `tests/upgrade_restart_tier0.rs`). An embedded
/// format's version moves with its carrier (e.g. `raftkv-wal` v2 inside the
/// v2 `control-wal`); none needs a transcode of its own yet.
///
/// Engine-resident row values and key encodings use `lsm-sstable` as their
/// carrier (they also pass through `lsm-wal` and the `raftkv`/`shared-wal`
/// payloads on the way in); the first transcode that needs them re-encodes
/// them wherever the engine holds them.
pub static EMBEDDED: &[Embedded] = &[
    // animus-control: records inside the control WAL (and `raftkv.wal*`,
    // which share the `CWL1` tag) and the node's system-keyspace engine.
    emb(
        "control-snapshot",
        Carrier::Table("control-wal"),
        "animus-control",
    ),
    emb("metadata", Carrier::Table("control-wal"), "animus-control"),
    emb(
        "mirror-version",
        Carrier::Table("lsm-sstable"),
        "animus-control",
    ),
    emb(
        "mirror-entities",
        Carrier::Table("lsm-sstable"),
        "animus-control",
    ),
    // animus-cp-data: the codec payload inside `raftkv.wal.<tablet>` /
    // `SharedWal` lines, the per-tablet engine layout, off-disk objects.
    // v2: the `CWL1` v2 framing its lines carry (sync markers, issue #1132);
    // its payload is unchanged, so `control-wal`'s transcode covers it.
    emb_v(
        "raftkv-wal",
        Carrier::Table("control-wal"),
        "animus-cp-data",
        2,
    ),
    emb("raftkv-wire", Carrier::OffDisk, "animus-cp-data"),
    emb("raftkv-image", Carrier::OffDisk, "animus-cp-data"),
    emb(
        "cp-engine-layout",
        Carrier::Table("lsm-sstable"),
        "animus-cp-data",
    ),
    emb("segment", Carrier::OffDisk, "animus-cp-data"),
    emb("backup-manifest", Carrier::OffDisk, "animus-cp-data"),
    emb("backup-data", Carrier::OffDisk, "animus-cp-data"),
    // animus-item: untagged frozen row values and pinned key vectors.
    emb("stored-item", Carrier::Table("lsm-sstable"), "animus-item"),
    emb(
        "change-record",
        Carrier::Table("lsm-sstable"),
        "animus-item",
    ),
    emb("key-bytes", Carrier::Table("lsm-sstable"), "animus-item"),
    emb("numkey", Carrier::Table("lsm-sstable"), "animus-item"),
    // animus-tablet: pinned key-space vectors.
    emb("escape", Carrier::Table("lsm-sstable"), "animus-tablet"),
    emb(
        "partition-token",
        Carrier::Table("lsm-sstable"),
        "animus-tablet",
    ),
    // animus-env: transient wire handshakes.
    emb("network-handshake", Carrier::OffDisk, "animus-env"),
    emb("client-handshake", Carrier::OffDisk, "animus-env"),
    // animusd / animus-operator: process config and the CR.
    emb("cluster-config", Carrier::OffDisk, "animusd"),
    emb("animuscluster-spec", Carrier::OffDisk, "animus-operator"),
];

/// The table entry named `name`.
#[must_use]
pub fn entry(name: &str) -> Option<&'static FormatEntry> {
    TABLE.iter().find(|e| e.name == name)
}

/// The `target_back` values (versions behind current, `0` = current) every
/// format in the table supports, ascending. Today `[0]`: only the identity.
/// Corpora pick their "k versions back" cells from this, so they grow with the
/// table and never name a version.
#[must_use]
pub fn supported_back() -> &'static [u32] {
    static BACK: OnceLock<Vec<u32>> = OnceLock::new();
    BACK.get_or_init(|| {
        let deepest = TABLE.iter().map(|e| e.current_version).max().unwrap_or(0);
        (0..deepest)
            .filter(|&k| {
                TABLE.iter().all(|e| {
                    k < e.current_version && e.capabilities(e.current_version - k).is_some()
                })
            })
            .collect()
    })
}

/// Classify one file on a node's disk: the format it holds, from its name and,
/// for the line-framed WALs, its magic (`<crc32 8 hex>:<MAGIC 4><version 2 hex>`).
/// `None` means not a table format (left untouched by [`transcode_disk`]).
///
/// Names recognised: `MANIFEST`, `wal-NNNNNN`, `sst-NNNNNN` (any directory or
/// prefix before the last `/` or as a leading prefix of the LSM names),
/// `raft.wal`, `raftkv.wal`, `raftkv.wal.<tablet>` and `raftkv.wal.shared`.
#[must_use]
pub fn classify(file: &str, bytes: &[u8]) -> Option<&'static FormatEntry> {
    // Content first for the line-framed tagged WALs: magic beats name.
    if bytes.len() >= 13 && bytes[8] == b':' {
        match &bytes[9..13] {
            b"CWL1" => return entry("control-wal"),
            b"SWL1" => return entry("shared-wal"),
            _ => {}
        }
    }
    if bytes.len() >= 4 && &bytes[..4] == b"ADE1" {
        return entry("encryption-envelope");
    }
    let base = file.rsplit('/').next().unwrap_or(file);
    // LSM file names are `{prefix}MANIFEST` / `{prefix}wal-NNNNNN` /
    // `{prefix}sst-NNNNNN`, where the prefix may be glued on without a `/`.
    if base.ends_with("MANIFEST") {
        return entry("lsm-manifest");
    }
    if ends_with_numbered(base, "wal-") {
        return entry("lsm-wal");
    }
    if ends_with_numbered(base, "sst-") {
        return entry("lsm-sstable");
    }
    // Empty (or not yet sniffable) line-framed WAL files, by name.
    if base == "raft.wal" || base == "raftkv.wal" || base.starts_with("raftkv.wal.") {
        return if base.ends_with(".shared") {
            entry("shared-wal")
        } else {
            entry("control-wal")
        };
    }
    None
}

fn ends_with_numbered(base: &str, stem: &str) -> bool {
    base.rfind(stem).is_some_and(|i| {
        let digits = &base[i + stem.len()..];
        digits.len() == 6 && digits.bytes().all(|b| b.is_ascii_digit())
    })
}

/// How [`transcode_disk`] treats a node's files.
#[derive(Clone, Debug, Default)]
pub struct TranscodeOpts {
    /// Per-mille of recognised files deliberately left at the *current*
    /// version (mixed-version files: one engine holds several versions).
    /// `0` transcodes every recognised file; `1000` none.
    pub keep_current_fraction_permille: u32,
    /// Stop after transcoding this many files, leaving the rest untouched
    /// (a crash inside the transcode window). `None` runs to completion.
    pub stop_after_files: Option<usize>,
    /// Seed for the per-file keep/transcode decision. Mixed with the file
    /// name through splitmix64; the simulator's RNG is never drawn, so calling
    /// this never perturbs a run's other random choices.
    pub seed: u64,
}

/// What a [`transcode_disk`] pass did, every list in file-name order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TranscodeReport {
    /// Files rewritten through their format's transcode, as
    /// `(file, format, target version)`. A rewrite to the same bytes (the
    /// identity) still counts: the pass went through the table.
    pub transcoded: Vec<(String, &'static str, u32)>,
    /// Recognised files deliberately left at the current version.
    pub kept: Vec<(String, &'static str)>,
    /// Files no table format claims, untouched.
    pub unrecognised: Vec<String>,
    /// Recognised files not reached because `stop_after_files` ended the pass.
    pub not_reached: Vec<(String, &'static str)>,
}

fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Whether `file` stays at the current version under `opts`. A pure function
/// of `(seed, file)`, so replays decide identically.
#[must_use]
pub fn keeps_current(opts: &TranscodeOpts, file: &str) -> bool {
    let roll = splitmix64(opts.seed ^ name_seed(file)) % 1000;
    roll < u64::from(opts.keep_current_fraction_permille)
}

/// Transcode every recognised durable file on `disk` back `target_back`
/// versions (`0` = current). Files are visited in sorted name order; each is
/// read, classified, transcoded and rewritten with the atomic
/// [`Disk::replace`]. The node must be stopped: nothing here coordinates with a
/// running engine.
///
/// A target no recognised format supports fails the whole pass with
/// [`TranscodeError::UnsupportedTarget`] before any file is rewritten.
///
/// # Errors
/// An unsupported target, a malformed file, or a disk error.
pub async fn transcode_disk_async<D: Disk>(
    disk: &D,
    target_back: u32,
    opts: &TranscodeOpts,
) -> Result<TranscodeReport, TranscodeError> {
    let io_err = |e: io::Error| TranscodeError::Io(e.to_string());
    let mut files = disk.list().await.map_err(io_err)?;
    files.sort();
    files.dedup();

    let mut plan: Vec<(String, &'static FormatEntry, Vec<u8>)> = Vec::new();
    let mut report = TranscodeReport::default();
    for file in files {
        let bytes = disk.read(&file).await.map_err(io_err)?;
        match classify(&file, &bytes) {
            Some(entry) => plan.push((file, entry, bytes)),
            None => report.unrecognised.push(file),
        }
    }
    // Refuse an unsupported target up front, so a failed request never leaves
    // a half-transcoded disk.
    for (_, entry, _) in &plan {
        let target = entry.current_version.saturating_sub(target_back);
        if target_back >= entry.current_version || entry.capabilities(target).is_none() {
            return Err(entry.unsupported(target));
        }
    }

    let mut done = 0usize;
    for (file, entry, bytes) in plan {
        if opts.stop_after_files.is_some_and(|n| done >= n) {
            report.not_reached.push((file, entry.name));
            continue;
        }
        if keeps_current(opts, &file) {
            report.kept.push((file, entry.name));
            continue;
        }
        let target = entry.current_version - target_back;
        let out = entry.transcode_to(&bytes, target)?;
        disk.replace(&file, &out).await.map_err(io_err)?;
        report.transcoded.push((file, entry.name, target));
        done += 1;
    }
    Ok(report)
}

/// Synchronous [`transcode_disk_async`] over a **stopped** node's `SimEnv`,
/// driven with `block_on` (a `SimEnv` disk operation completes without the
/// simulator being stepped, the pattern the storage crash tests use).
///
/// # Errors
/// As [`transcode_disk_async`], as an `InvalidInput`/I-O `io::Error`.
pub fn transcode_disk(
    env: &SimEnv,
    target_back: u32,
    opts: &TranscodeOpts,
) -> io::Result<TranscodeReport> {
    block_on(transcode_disk_async(env, target_back, opts)).map_err(|e| match e {
        TranscodeError::Io(m) => io::Error::other(m),
        other => other.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_is_well_formed() {
        let mut names: Vec<&str> = TABLE.iter().map(|e| e.name).collect();
        names.sort_unstable();
        let n = names.len();
        names.dedup();
        assert_eq!(n, names.len(), "duplicate table entry");
        for e in TABLE {
            assert!(
                e.capabilities(e.current_version).is_some(),
                "{}: current version must be listed",
                e.name
            );
            assert_eq!(
                e.versions.last().map(|v| v.version),
                Some(e.current_version),
                "{}: versions must end with current",
                e.name
            );
            assert!(e.versions.windows(2).all(|w| w[0].version < w[1].version));
        }
    }

    #[test]
    fn unsupported_target_is_an_error_never_identity() {
        for e in TABLE {
            for target in [0, e.current_version + 1, 99] {
                match e.transcode_to(b"anything", target) {
                    Err(TranscodeError::UnsupportedTarget { .. }) => {}
                    other => panic!(
                        "{} -> v{target}: expected UnsupportedTarget, got {other:?}",
                        e.name
                    ),
                }
            }
            assert_eq!(
                e.transcode_to(b"anything", e.current_version).unwrap(),
                b"anything"
            );
        }
    }

    #[test]
    fn supported_back_is_identity_only_today() {
        assert_eq!(supported_back(), &[0]);
    }

    #[test]
    fn classifies_node_files() {
        let cases: &[(&str, &[u8], Option<&str>)] = &[
            ("lsm/MANIFEST", b"", Some("lsm-manifest")),
            ("lsm/wal-000003", b"", Some("lsm-wal")),
            ("lsm/sst-000012", b"", Some("lsm-sstable")),
            ("t7-wal-000001", b"", Some("lsm-wal")),
            ("raft.wal", b"", Some("control-wal")),
            ("raftkv.wal", b"", Some("control-wal")),
            ("raftkv.wal.4", b"", Some("control-wal")),
            ("raftkv.wal.shared", b"", Some("shared-wal")),
            ("whatever", b"deadbeef:SWL101{}\n", Some("shared-wal")),
            ("whatever", b"deadbeef:CWL101{}\n", Some("control-wal")),
            ("wal-12", b"", None),
            ("notes.txt", b"hello", None),
        ];
        for (file, bytes, want) in cases {
            assert_eq!(classify(file, bytes).map(|e| e.name), *want, "{file}");
        }
    }
}
