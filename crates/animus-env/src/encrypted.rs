//! Encryption at rest (ADR 0069): a frame-sealing AEAD wrapper over any
//! [`Disk`] implementation, plus the per-node [`EncryptionKey`] type and the
//! directory-level "loud refusal" check ([`verify_or_init_marker`]) that
//! catches a key/data-directory mismatch before any WAL or engine file is
//! opened for use.
//!
//! # Format
//!
//! Every file this wrapper writes starts with a 21-byte header —
//! `MAGIC(4) || VERSION(1) || SALT(16)` — followed by zero or more
//! self-delimited **frames**, one per [`Disk::append`] call (or exactly one
//! for a [`Disk::replace`], which always mints a fresh header): `LEN(4, big-
//! endian u32) || CIPHERTEXT_AND_TAG(LEN bytes)`. `LEN` is the length of the
//! XChaCha20-Poly1305 ciphertext plus its 16-byte authentication tag; the
//! plaintext frame length is `LEN - 16`.
//!
//! The nonce for frame `i` of a file is `SALT || i.to_be_bytes()` (16 + 8 =
//! 24 bytes, exactly XChaCha20-Poly1305's nonce size) — deterministic and
//! unique as long as `SALT` is fresh per file generation, which this module
//! guarantees: [`Disk::append`] mints a new random salt only when the file
//! does not exist yet (frame indices increment monotonically after that, so
//! `(salt, counter)` — and therefore the nonce — never repeats for the life
//! of that file generation), and [`Disk::replace`] *always* mints a brand
//! new salt (so a compaction/manifest rewrite never reuses the nonce space
//! of whatever generation of the file preceded it, even if the new content
//! happens to start again at counter 0).
//!
//! # Loud refusal
//!
//! [`verify_or_init_marker`] is the ADR 0069 counterpart of
//! `animus_cp_data::host::check_wal_layout`'s loud layout-mismatch refusal:
//! called once, before any WAL/engine file is opened, it lists the data
//! directory and reads at most one small marker file
//! ([`MARKER_FILE`]) to decide whether the directory is "fresh", "already
//! encrypted", or "already plaintext" — and refuses (a plain `io::Error`,
//! never a silent reset or partial start) on any of the two mismatched
//! combinations: a key given against an existing plaintext directory, or a
//! missing/wrong key against an existing encrypted one. See its own doc for
//! the exact decision table and error text.
//!
//! # Torn tails
//!
//! A crash mid-`append` can leave a file whose last frame is short (the
//! length prefix or ciphertext bytes never made it to disk) or fails AEAD
//! authentication (the tag was only partially written). Once
//! [`verify_or_init_marker`] has confirmed the configured key is the right
//! one for this directory, *any* frame-parse failure encountered while
//! scanning an ordinary file can only mean "this is where the last
//! unsynced write was cut off" — never "wrong key" (that would already have
//! been caught by the marker, whose own single frame is written durably via
//! [`Disk::replace`] and therefore can never itself be torn, see
//! [`Disk::replace`]'s crash contract). So this module treats the first
//! frame-parse failure encountered during a scan, whatever its specific
//! cause, uniformly as a torn tail: everything up to (not including) that
//! frame is the valid prefix, and [`EncryptedDisk`] physically truncates the
//! file back to it (via [`Disk::replace`]) the first time that file is
//! touched after a restart — a strict subset of the byte-level torn tails
//! the WAL/LSM layers already recover from, since a whole frame (not an
//! arbitrary byte range) is always discarded as a unit.
//!
//! # Concurrency
//!
//! [`EncryptedDisk`] follows the same "cache lock never held across an
//! `.await`" discipline [`ProdEnv`](crate::ProdEnv)'s own `dir_synced`/
//! `conns` maps already use — but does **not** itself serialize concurrent
//! `append`/`replace` calls to the *same* file name: two callers racing an
//! append to one file could compute the same next frame counter and reuse a
//! nonce. This is safe today because every real caller already serializes
//! writes to a given file by construction (WAL group-commit, the control
//! apply task, SSTable/manifest writers are each single-writer-per-file) —
//! the identical precondition the plaintext `Disk` implementations already
//! rely on for `append`'s own "buffered, appended in call order" contract.
//! A future caller that breaks this precondition needs its own
//! serialization, exactly as it would today without encryption.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::sync::Mutex as StdMutex;

use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{Key as AeadKey, XChaCha20Poly1305, XNonce};
use zeroize::Zeroizing;

use crate::{
    BoxFuture, Clock, Disk, Env, MetricsHandle, Nanos, Network, NodeId, Rng, Spawner, UnixMillis,
};

const MAGIC: [u8; 4] = *b"ADE1";
const VERSION: u8 = 1;
const SALT_LEN: usize = 16;
const HEADER_LEN: usize = 4 + 1 + SALT_LEN; // 21
const TAG_LEN: usize = 16;
const NONCE_LEN: usize = 24;
const LEN_PREFIX: usize = 4;

/// The marker file name recording whether a data directory is encrypted
/// (ADR 0069). Filtered out of [`EncryptedDisk::list`] so it never appears
/// to a higher layer that enumerates its own kind-prefixed files.
pub const MARKER_FILE: &str = ".animus_encryption_marker";
const MARKER_PLAINTEXT: &[u8] = b"ANIMUSDB-ENCRYPTION-MARKER-V1";

/// A 256-bit AEAD key loaded from a per-node key file (`--encryption-key
/// PATH`, ADR 0069). Never printed in full (`Debug` redacts it); the byte
/// buffer is [`Zeroizing`], so it is scrubbed from memory on drop.
#[derive(Clone)]
pub struct EncryptionKey(Zeroizing<[u8; 32]>);

impl EncryptionKey {
    /// Wrap a raw 256-bit key already in memory.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        EncryptionKey(Zeroizing::new(bytes))
    }

    /// Load a key from `path`: 64 hex characters (256 bits), optionally
    /// followed by a single trailing newline — the same "one secret, one
    /// file" shape `--dynamo-auth PATH`/`--tls-key PATH` use, chosen over
    /// PEM/JSON because a raw symmetric key has no structure to carry.
    /// Generate one with e.g. `openssl rand -hex 32 > key.hex`.
    ///
    /// Real (blocking) file I/O — like `load_dynamo_auth_file`, this runs
    /// once at CLI startup before any `Env` exists, not on a hot path
    /// through the `Disk` seam.
    pub fn load_from_file(path: &Path) -> std::io::Result<Self> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            std::io::Error::new(e.kind(), format!("reading {}: {e}", path.display()))
        })?;
        let hex = text.trim();
        if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "encryption key file {} must contain exactly 64 hex characters (256 bits); \
                     generate one with `openssl rand -hex 32 > {}`",
                    path.display(),
                    path.display()
                ),
            ));
        }
        let mut bytes = [0u8; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            // Already validated as ASCII hex above, so this cannot fail.
            *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).expect("validated hex digits");
        }
        Ok(EncryptionKey::from_bytes(bytes))
    }

    fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new(AeadKey::from_slice(self.0.as_slice()))
    }
}

impl fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EncryptionKey(REDACTED)")
    }
}

fn derive_nonce(salt: &[u8; SALT_LEN], counter: u64) -> [u8; NONCE_LEN] {
    let mut n = [0u8; NONCE_LEN];
    n[..SALT_LEN].copy_from_slice(salt);
    n[SALT_LEN..].copy_from_slice(&counter.to_be_bytes());
    n
}

fn random_salt<R: Rng + ?Sized>(rng: &R) -> [u8; SALT_LEN] {
    let mut s = [0u8; SALT_LEN];
    rng.fill_bytes(&mut s);
    s
}

fn make_header(salt: &[u8; SALT_LEN]) -> Vec<u8> {
    let mut h = Vec::with_capacity(HEADER_LEN);
    h.extend_from_slice(&MAGIC);
    h.push(VERSION);
    h.extend_from_slice(salt);
    h
}

/// Seal `plaintext` as frame `counter` of the file salted with `salt`:
/// `LEN(4) || ciphertext || tag`.
fn seal_frame(
    key: &EncryptionKey,
    salt: &[u8; SALT_LEN],
    counter: u64,
    plaintext: &[u8],
) -> Vec<u8> {
    let nonce = derive_nonce(salt, counter);
    let ct = key
        .cipher()
        .encrypt(XNonce::from_slice(&nonce), plaintext)
        .expect("XChaCha20-Poly1305 encryption only fails on a plaintext exceeding ~256 GiB");
    let mut framed = Vec::with_capacity(LEN_PREFIX + ct.len());
    framed.extend_from_slice(
        &u32::try_from(ct.len())
            .expect("a single Disk::append/replace call never carries >4 GiB")
            .to_be_bytes(),
    );
    framed.extend_from_slice(&ct);
    framed
}

/// Open ciphertext (tag included, no length prefix) for frame `counter`.
/// `None` on authentication failure — see the module doc's "Torn tails"
/// section for how callers interpret that once the marker has already
/// validated the key.
fn open_frame(
    key: &EncryptionKey,
    salt: &[u8; SALT_LEN],
    counter: u64,
    ciphertext: &[u8],
) -> Option<Vec<u8>> {
    let nonce = derive_nonce(salt, counter);
    key.cipher()
        .decrypt(XNonce::from_slice(&nonce), ciphertext)
        .ok()
}

#[derive(Clone)]
struct FrameEntry {
    /// Byte offset, in the raw (encrypted) file, of this frame's length
    /// prefix.
    raw_offset: u64,
    /// Total raw bytes this frame occupies, length prefix included.
    raw_len: u32,
    plain_offset: u64,
    plain_len: u32,
}

#[derive(Clone)]
struct FileIndex {
    salt: [u8; SALT_LEN],
    frames: Vec<FrameEntry>,
    plain_len: u64,
    /// Raw bytes on disk this index actually accounts for (header + every
    /// frame). `0` means "no bytes physically written yet for this
    /// generation of the file" (a brand-new salt minted for a file that
    /// does not exist yet); `Disk::append`'s next physical write prepends
    /// the header in that case.
    raw_len: u64,
}

enum Scan {
    /// The file does not exist (or is empty) — no header to speak of.
    Absent,
    /// Bytes are present but don't start with the expected magic — either a
    /// plaintext file (directory-level mismatch) or a corrupted header.
    NotEncrypted,
    /// The full header is present with the right `ADE1` magic, but its
    /// version byte is outside `1..=VERSION` (0, or written by a newer
    /// binary). Never a torn file and never decodable with v1 logic: it
    /// surfaces as a loud, typed error (ADR 0073 Phase 0).
    UnsupportedVersion(u8),
    /// A parseable (possibly torn) encrypted file: `index` covers every
    /// frame up to (not including) the first parse/auth failure, if any;
    /// `torn` says whether such a failure was found — safe to recover from
    /// by truncating to `index` (see the module doc's "Torn tails"
    /// section).
    Ok { index: FileIndex, torn: bool },
    /// A frame failed to parse/authenticate, and a **later** frame in the
    /// same raw file went on to authenticate successfully. A genuine
    /// crash-torn write, by definition, cuts off at the true end of the
    /// file — nothing valid can follow the tear. A failure with valid data
    /// *after* it can therefore only be real corruption of already-durable
    /// bytes (e.g. bit rot, or `Simulator::corrupt_durable` in a test), and
    /// recovery must refuse loudly rather than silently discard whatever
    /// came after the corrupted frame.
    Corrupted,
}

fn scan(key: &EncryptionKey, raw: &[u8]) -> Scan {
    if raw.is_empty() {
        return Scan::Absent;
    }
    if raw.len() < HEADER_LEN || raw[0..4] != MAGIC {
        return Scan::NotEncrypted;
    }
    // Version dispatch (ADR 0073 Phase 1): exact `match`, never a range check.
    // When `VERSION` becomes 2, `2 => scan_v2(..)` joins here and the v1 arm
    // moves to `legacy::v1` (see `legacy`).
    match raw[4] {
        1 => scan_v1(key, raw),
        v => Scan::UnsupportedVersion(v),
    }
}

/// Legacy (pre-current-version) ADE1 envelope scanners (ADR 0073 Phase 1,
/// "upgrade-on-read"). Empty while the envelope is still v1. Once version N+1
/// exists, `scan_vN` moves to `legacy::vN` unchanged in behavior and — the
/// upgrade-on-read contract — yields the *current* in-memory `Scan`/
/// `FileIndex`; `scan`'s `match` routes to it. Never deleted (support
/// window: forever).
mod legacy {}

/// The v1 envelope scan: `raw` is known to start with a full, v1 header.
fn scan_v1(key: &EncryptionKey, raw: &[u8]) -> Scan {
    let mut salt = [0u8; SALT_LEN];
    salt.copy_from_slice(&raw[5..HEADER_LEN]);

    // Only frames collected before the first failure are trustworthy; scanning
    // continues past a failure purely to tell a trailing tear from real
    // mid-file corruption (see `Scan::Corrupted`'s doc), never adding
    // anything more to `trusted`.
    let mut trusted = Vec::new();
    let mut trusted_plain_len: u64 = 0;
    let mut trusted_raw_len: u64 = HEADER_LEN as u64;

    let mut pos: usize = HEADER_LEN;
    let mut counter: u64 = 0;
    let mut failed = false;

    loop {
        if pos == raw.len() {
            break;
        }
        if pos + LEN_PREFIX > raw.len() {
            failed = true;
            break;
        }
        let ct_len =
            u32::from_be_bytes(raw[pos..pos + LEN_PREFIX].try_into().expect("4 bytes")) as usize;
        let body_start = pos + LEN_PREFIX;
        let body_end = match body_start.checked_add(ct_len) {
            Some(v) => v,
            None => {
                failed = true;
                break;
            }
        };
        if ct_len < TAG_LEN || body_end > raw.len() {
            failed = true;
            break;
        }
        match open_frame(key, &salt, counter, &raw[body_start..body_end]) {
            Some(plaintext) => {
                if failed {
                    // Authenticated *after* an earlier failure: that
                    // earlier failure cannot have been a trailing tear.
                    return Scan::Corrupted;
                }
                trusted.push(FrameEntry {
                    raw_offset: pos as u64,
                    raw_len: (LEN_PREFIX + ct_len) as u32,
                    plain_offset: trusted_plain_len,
                    plain_len: plaintext.len() as u32,
                });
                trusted_plain_len += plaintext.len() as u64;
                trusted_raw_len = body_end as u64;
                counter += 1;
                pos = body_end;
            }
            None => {
                // Keep exploring past this frame using its own (still
                // self-consistent) length field, purely to classify the
                // failure; `trusted` is not touched again.
                failed = true;
                counter += 1;
                pos = body_end;
            }
        }
    }

    Scan::Ok {
        index: FileIndex {
            salt,
            frames: trusted,
            plain_len: trusted_plain_len,
            raw_len: trusted_raw_len,
        },
        torn: failed,
    }
}

fn mixed_state_error(file: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "{file} is not in the expected encrypted frame format even though this data \
             directory's {MARKER_FILE} says it is encrypted — the data directory is corrupted \
             or was partially migrated by hand; refusing to use it"
        ),
    )
}

/// Distinct from [`mixed_state_error`] and [`corruption_error`]: the file
/// is a genuine `ADE1` envelope of a version this binary cannot read.
/// `InvalidData` like its siblings; the message text is the discriminator.
fn unsupported_version_error(file: &str, found: u8) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "{file}: unsupported ADE1 encryption envelope version {found} (this binary supports \
             versions 1..={VERSION}); refusing to read it — it was written by a newer binary or \
             is corrupt"
        ),
    )
}

fn corruption_error(file: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "{file}: an encrypted frame failed to decrypt where a valid one was expected (corruption)"
        ),
    )
}

/// Seal `plaintext` as a whole, standalone object: a fresh header (random
/// salt) plus exactly one frame at counter 0 — the identical shape
/// [`Disk::replace`]'s own physical payload takes. Used both by the
/// marker file/object (below and in `encrypted_segment_store.rs`) and by
/// [`crate::EncryptedSegmentStore::put`] (ADR 0069 PR 2), which seals each
/// write-once object this same way: no frame index is needed since a
/// `SegmentStore` object, unlike a `Disk` file, is always read/written
/// whole, never incrementally appended.
pub(crate) fn seal_whole<R: Rng + ?Sized>(
    key: &EncryptionKey,
    rng: &R,
    plaintext: &[u8],
) -> Vec<u8> {
    let salt = random_salt(rng);
    let framed = seal_frame(key, &salt, 0, plaintext);
    let mut out = make_header(&salt);
    out.extend_from_slice(&framed);
    out
}

/// Open a whole standalone object sealed by [`seal_whole`]. `None` on any
/// parse/authentication failure — the caller decides what that means (a
/// wrong key, corruption, or genuinely not-our-format bytes). Used both by
/// the marker file/object's own authentication check and by
/// [`crate::EncryptedSegmentStore::get`], which — unlike
/// [`EncryptedDisk`]'s per-file frame scan — has no torn-tail case to
/// consider at all: a `SegmentStore` object is never incrementally
/// appended, so any failure here is either a key/store mismatch or
/// genuine corruption, never a legitimate crash-torn write in progress.
pub(crate) fn open_whole(key: &EncryptionKey, raw: &[u8]) -> Option<Vec<u8>> {
    if raw.len() <= HEADER_LEN + LEN_PREFIX || raw[0..4] != MAGIC {
        return None;
    }
    let mut salt = [0u8; SALT_LEN];
    salt.copy_from_slice(&raw[5..HEADER_LEN]);
    let ct_len =
        u32::from_be_bytes(raw[HEADER_LEN..HEADER_LEN + LEN_PREFIX].try_into().ok()?) as usize;
    let body = &raw[HEADER_LEN + LEN_PREFIX..];
    if body.len() != ct_len {
        return None;
    }
    open_frame(key, &salt, 0, body)
}

fn seal_marker<R: Rng + ?Sized>(key: &EncryptionKey, rng: &R) -> Vec<u8> {
    seal_whole(key, rng, MARKER_PLAINTEXT)
}

/// A marker is written once via [`Disk::replace`], whose crash contract
/// ("a crash before or after sees the whole old or whole new contents,
/// never a mix") guarantees it is never torn — so unlike an ordinary data
/// file, *any* parse/authentication failure here means "wrong key or
/// corrupted marker", never "torn tail".
fn marker_authenticates(key: &EncryptionKey, raw: &[u8]) -> bool {
    matches!(open_whole(key, raw), Some(pt) if pt == MARKER_PLAINTEXT)
}

/// The ADR 0069 loud-refusal check: decide (from a directory listing plus
/// at most one marker read) whether `disk`'s data directory and `key` are
/// compatible, initializing the marker on a fresh directory. Call this
/// **before** constructing an [`EncryptedDisk`]/[`EncryptedEnv`] and before
/// any WAL/engine file in the same directory is opened for use.
///
/// | directory has [`MARKER_FILE`] | `key` given | outcome |
/// |---|---|---|
/// | yes | no | **refuse**: encrypted directory, no key |
/// | yes | yes, authenticates | proceed encrypted |
/// | yes | yes, does not authenticate | **refuse**: wrong key |
/// | no, but has other files | yes | **refuse**: key against a plaintext directory |
/// | no, but has other files | no | proceed plaintext (today's behavior, untouched) |
/// | no other files (fresh/empty) | yes | initialize the marker, proceed encrypted |
/// | no other files (fresh/empty) | no | proceed plaintext (today's behavior, untouched) |
pub async fn verify_or_init_marker<D: Disk + ?Sized, R: Rng + ?Sized>(
    disk: &D,
    rng: &R,
    key: Option<&EncryptionKey>,
) -> std::io::Result<()> {
    let names = disk.list().await?;
    let marker_present = names.iter().any(|n| n == MARKER_FILE);
    let has_other_files = names.iter().any(|n| n != MARKER_FILE);
    match (marker_present, key) {
        (true, None) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "data directory is encrypted (found {MARKER_FILE}) but no --encryption-key was \
                 given — refusing to start. Pass --encryption-key PATH with the same key this \
                 directory was created with, or point at a fresh data directory."
            ),
        )),
        (true, Some(k)) => {
            let raw = disk.read(MARKER_FILE).await?;
            if marker_authenticates(k, &raw) {
                Ok(())
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "--encryption-key does not match the key this data directory was encrypted \
                     with — refusing to start. Use the original key, or point at a fresh data \
                     directory."
                        .to_string(),
                ))
            }
        }
        (false, Some(_)) if has_other_files => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "--encryption-key was given but this data directory already holds unencrypted \
                 files (no {MARKER_FILE} marker) — refusing to start. Encryption cannot be \
                 enabled on an existing plaintext data directory; point at a fresh data \
                 directory instead."
            ),
        )),
        (false, Some(k)) => {
            let marker = seal_marker(k, rng);
            disk.replace(MARKER_FILE, &marker).await
        }
        (false, None) => Ok(()),
    }
}

struct EncryptedDiskState<D, R> {
    inner: D,
    rng: R,
    key: EncryptionKey,
    cache: StdMutex<BTreeMap<String, FileIndex>>,
}

/// An AEAD-encrypting [`Disk`] wrapper, generic over any underlying `D:
/// Disk` (so `SimEnv`'s crash/fault corpora can drive it deterministically)
/// and any `R: Rng` source for minting per-file/per-`replace` salts. See
/// the module doc for the frame format, loud-refusal check, and torn-tail
/// recovery. Cheap to clone (an `Arc`-backed handle, like
/// [`ProdEnv`](crate::ProdEnv)/`SimEnv` themselves) — clones share the same
/// in-memory frame-index cache.
///
/// Construct only after [`verify_or_init_marker`] has confirmed `key` is
/// right for `inner`'s data directory — this type itself performs no
/// marker/loud-refusal check, only per-file frame bookkeeping.
pub struct EncryptedDisk<D, R> {
    shared: std::sync::Arc<EncryptedDiskState<D, R>>,
}

impl<D, R> Clone for EncryptedDisk<D, R> {
    fn clone(&self) -> Self {
        EncryptedDisk {
            shared: std::sync::Arc::clone(&self.shared),
        }
    }
}

impl<D: Disk, R: Rng> EncryptedDisk<D, R> {
    /// Wrap `inner`, sealing every write under `key` and drawing per-file
    /// salts from `rng`.
    pub fn new(inner: D, rng: R, key: EncryptionKey) -> Self {
        EncryptedDisk {
            shared: std::sync::Arc::new(EncryptedDiskState {
                inner,
                rng,
                key,
                cache: StdMutex::new(BTreeMap::new()),
            }),
        }
    }

    /// The wrapped disk.
    pub fn inner(&self) -> &D {
        &self.shared.inner
    }

    async fn get_index(&self, file: &str) -> std::io::Result<FileIndex> {
        if let Some(idx) = self
            .shared
            .cache
            .lock()
            .expect("encrypted disk cache poisoned")
            .get(file)
            .cloned()
        {
            return Ok(idx);
        }
        let idx = self.load_or_init_index(file).await?;
        self.shared
            .cache
            .lock()
            .expect("encrypted disk cache poisoned")
            .insert(file.to_string(), idx.clone());
        Ok(idx)
    }

    fn put_index(&self, file: &str, idx: FileIndex) {
        self.shared
            .cache
            .lock()
            .expect("encrypted disk cache poisoned")
            .insert(file.to_string(), idx);
    }

    /// Build a file's index from scratch: reads it once, and — if scanning
    /// finds a torn or trailing-garbage tail — physically truncates the
    /// file back to the last complete frame (see the module doc's "Torn
    /// tails" section) so a subsequent `append` never strands that garbage
    /// mid-file.
    async fn load_or_init_index(&self, file: &str) -> std::io::Result<FileIndex> {
        let raw = self.shared.inner.read(file).await?;
        match scan(&self.shared.key, &raw) {
            Scan::Absent => Ok(FileIndex {
                salt: random_salt(&self.shared.rng),
                frames: Vec::new(),
                plain_len: 0,
                raw_len: 0,
            }),
            Scan::NotEncrypted => Err(mixed_state_error(file)),
            Scan::UnsupportedVersion(found) => Err(unsupported_version_error(file, found)),
            Scan::Corrupted => Err(corruption_error(file)),
            Scan::Ok { index, torn } => {
                if torn || (index.raw_len as usize) < raw.len() {
                    let valid = &raw[..index.raw_len as usize];
                    self.shared.inner.replace(file, valid).await?;
                }
                Ok(index)
            }
        }
    }
}

#[async_trait::async_trait]
impl<D: Disk, R: Rng> Disk for EncryptedDisk<D, R> {
    async fn append(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        let idx = self.get_index(file).await?;
        let counter = idx.frames.len() as u64;
        let framed = seal_frame(&self.shared.key, &idx.salt, counter, bytes);
        let mut physical = Vec::new();
        let frame_raw_offset = if idx.raw_len == 0 {
            physical.extend_from_slice(&make_header(&idx.salt));
            HEADER_LEN as u64
        } else {
            idx.raw_len
        };
        physical.extend_from_slice(&framed);
        self.shared.inner.append(file, &physical).await?;

        let mut idx = idx;
        idx.frames.push(FrameEntry {
            raw_offset: frame_raw_offset,
            raw_len: framed.len() as u32,
            plain_offset: idx.plain_len,
            plain_len: bytes.len() as u32,
        });
        idx.plain_len += bytes.len() as u64;
        idx.raw_len = frame_raw_offset + framed.len() as u64;
        self.put_index(file, idx);
        Ok(())
    }

    async fn sync(&self, file: &str) -> std::io::Result<()> {
        self.shared.inner.sync(file).await
    }

    async fn read(&self, file: &str) -> std::io::Result<Vec<u8>> {
        let idx = self.get_index(file).await?;
        let mut out = Vec::with_capacity(idx.plain_len as usize);
        for (counter, f) in idx.frames.iter().enumerate() {
            let raw = self
                .shared
                .inner
                .read_at(file, f.raw_offset, f.raw_len as usize)
                .await?;
            if raw.len() != f.raw_len as usize || raw.len() < LEN_PREFIX {
                return Err(corruption_error(file));
            }
            let ct = &raw[LEN_PREFIX..];
            let pt = open_frame(&self.shared.key, &idx.salt, counter as u64, ct)
                .ok_or_else(|| corruption_error(file))?;
            out.extend_from_slice(&pt);
        }
        Ok(out)
    }

    async fn read_at(&self, file: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        let idx = self.get_index(file).await?;
        if offset >= idx.plain_len || len == 0 {
            return Ok(Vec::new());
        }
        let end = offset.saturating_add(len as u64).min(idx.plain_len);
        let mut out = Vec::with_capacity((end - offset) as usize);
        for (counter, f) in idx.frames.iter().enumerate() {
            let f_start = f.plain_offset;
            let f_end = f.plain_offset + u64::from(f.plain_len);
            if f_end <= offset || f_start >= end {
                continue;
            }
            let raw = self
                .shared
                .inner
                .read_at(file, f.raw_offset, f.raw_len as usize)
                .await?;
            if raw.len() != f.raw_len as usize || raw.len() < LEN_PREFIX {
                return Err(corruption_error(file));
            }
            let ct = &raw[LEN_PREFIX..];
            let pt = open_frame(&self.shared.key, &idx.salt, counter as u64, ct)
                .ok_or_else(|| corruption_error(file))?;
            let lo = offset.saturating_sub(f_start) as usize;
            let hi = (end.min(f_end) - f_start) as usize;
            out.extend_from_slice(&pt[lo..hi]);
        }
        Ok(out)
    }

    async fn size(&self, file: &str) -> std::io::Result<u64> {
        Ok(self.get_index(file).await?.plain_len)
    }

    async fn remove(&self, file: &str) -> std::io::Result<()> {
        self.shared.inner.remove(file).await?;
        self.shared
            .cache
            .lock()
            .expect("encrypted disk cache poisoned")
            .remove(file);
        Ok(())
    }

    async fn replace(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        let salt = random_salt(&self.shared.rng);
        let framed = seal_frame(&self.shared.key, &salt, 0, bytes);
        let mut physical = make_header(&salt);
        physical.extend_from_slice(&framed);
        self.shared.inner.replace(file, &physical).await?;
        self.put_index(
            file,
            FileIndex {
                salt,
                frames: vec![FrameEntry {
                    raw_offset: HEADER_LEN as u64,
                    raw_len: framed.len() as u32,
                    plain_offset: 0,
                    plain_len: bytes.len() as u32,
                }],
                plain_len: bytes.len() as u64,
                raw_len: (HEADER_LEN + framed.len()) as u64,
            },
        );
        Ok(())
    }

    async fn list(&self) -> std::io::Result<Vec<String>> {
        let mut names = self.shared.inner.list().await?;
        names.retain(|n| n != MARKER_FILE);
        Ok(names)
    }

    async fn link(&self, src: &str, dst: &str) -> std::io::Result<()> {
        self.shared.inner.link(src, dst).await?;
        // `dst` now shares `src`'s exact encrypted bytes (a real hard link,
        // no re-encryption) — mirror the cached index if we have one so a
        // caller that immediately reads `dst` doesn't pay a rescan; an
        // absent cache entry is rebuilt lazily on first access either way.
        let mut cache = self
            .shared
            .cache
            .lock()
            .expect("encrypted disk cache poisoned");
        match cache.get(src).cloned() {
            Some(idx) => {
                cache.insert(dst.to_string(), idx);
            }
            None => {
                cache.remove(dst);
            }
        }
        Ok(())
    }
}

/// A drop-in [`Env`] wrapper (ADR 0069): every method except the `Disk`
/// seam delegates unchanged to the wrapped `env`; `Disk` methods route
/// through an [`EncryptedDisk`] sealing every write under `key`. This is
/// what lets `LsmEngine<EncryptedEnv<SimEnv>>` reuse the crash/fault
/// corpora already written for `LsmEngine<E>` — see
/// `animus-storage/tests/lsm_crash.rs` and `tests/lsm_disk_faults.rs`.
///
/// `ProdEnv` does **not** use this wrapper directly (see ADR 0069's
/// "Wiring into `animusd`" section for why) — it composes the same
/// [`EncryptedDisk`]/[`verify_or_init_marker`] machinery internally instead,
/// behind its own `Disk` impl, so every existing concrete `ProdEnv` call
/// site in `animusd` keeps compiling unchanged.
#[derive(Clone)]
pub struct EncryptedEnv<E: Env> {
    env: E,
    disk: EncryptedDisk<E, E>,
}

impl<E: Env> EncryptedEnv<E> {
    /// Wrap `env`, verifying/initializing the directory-level marker first
    /// (see [`verify_or_init_marker`]) — loudly refuses a key/directory
    /// mismatch before returning.
    pub async fn open(env: E, key: EncryptionKey) -> std::io::Result<Self> {
        verify_or_init_marker(&env, &env, Some(&key)).await?;
        let disk = EncryptedDisk::new(env.clone(), env.clone(), key);
        Ok(EncryptedEnv { env, disk })
    }

    /// The wrapped env (e.g. to reach `SimEnv`-only test helpers).
    pub fn inner(&self) -> &E {
        &self.env
    }
}

#[async_trait::async_trait]
impl<E: Env> Clock for EncryptedEnv<E> {
    fn now(&self) -> Nanos {
        self.env.now()
    }

    fn wall_now(&self) -> UnixMillis {
        self.env.wall_now()
    }

    async fn sleep(&self, dur: std::time::Duration) {
        self.env.sleep(dur).await;
    }
}

impl<E: Env> Rng for EncryptedEnv<E> {
    fn next_u64(&self) -> u64 {
        self.env.next_u64()
    }

    fn fill_bytes(&self, dst: &mut [u8]) {
        self.env.fill_bytes(dst);
    }
}

#[async_trait::async_trait]
impl<E: Env> Network for EncryptedEnv<E> {
    async fn send_stream(&self, to: NodeId, stream: u64, payload: Vec<u8>) {
        self.env.send_stream(to, stream, payload).await;
    }

    async fn recv_stream(&self, stream: u64) -> crate::Envelope {
        self.env.recv_stream(stream).await
    }

    fn close_stream(&self, stream: u64) {
        self.env.close_stream(stream);
    }

    fn set_require_peer_ext(&self, on: bool) {
        self.env.set_require_peer_ext(on);
    }
}

#[async_trait::async_trait]
impl<E: Env> Disk for EncryptedEnv<E> {
    async fn append(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        self.disk.append(file, bytes).await
    }

    async fn sync(&self, file: &str) -> std::io::Result<()> {
        self.disk.sync(file).await
    }

    async fn read(&self, file: &str) -> std::io::Result<Vec<u8>> {
        self.disk.read(file).await
    }

    async fn read_at(&self, file: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        self.disk.read_at(file, offset, len).await
    }

    async fn size(&self, file: &str) -> std::io::Result<u64> {
        self.disk.size(file).await
    }

    async fn remove(&self, file: &str) -> std::io::Result<()> {
        self.disk.remove(file).await
    }

    async fn replace(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        self.disk.replace(file, bytes).await
    }

    async fn list(&self) -> std::io::Result<Vec<String>> {
        self.disk.list().await
    }

    async fn link(&self, src: &str, dst: &str) -> std::io::Result<()> {
        self.disk.link(src, dst).await
    }
}

impl<E: Env> Spawner for EncryptedEnv<E> {
    fn spawn(&self, fut: BoxFuture<'static, ()>) {
        self.env.spawn(fut);
    }
}

impl<E: Env> Env for EncryptedEnv<E> {
    fn node_id(&self) -> NodeId {
        self.env.node_id()
    }

    fn metrics(&self) -> MetricsHandle {
        self.env.metrics()
    }
}

/// ADR 0073 Phase 0, Workstream A: the golden fixture for this module's
/// `ADE1` envelope. `ADE1` already carries `MAGIC || VERSION` (see the
/// module doc), so there is no reset here — only a checked-in fixture plus
/// the decode/round-trip/generator tests the ADR's "Phase 0 conventions"
/// section (`docs/adr/0073-upgrade-compatibility.md`) prescribes for every
/// format in the inventory.
///
/// This crate has no dependency on `animus-sim`/`tokio` outside the `prod`
/// feature (see `Cargo.toml`'s comments), so this module brings its own
/// tiny, fully synchronous `Disk`/`Rng` test doubles and its own
/// single-poll `block_on` — deliberately not `SimEnv` or `#[tokio::test]` —
/// so these tests run under a plain `cargo test -p animus-env`, no features
/// required, exactly like every other per-push gate in this crate.
#[cfg(test)]
mod format_fixture_tests {
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::path::PathBuf;
    use std::sync::Mutex as StdMutex;
    use std::task::{Context, Poll, Waker};

    use super::{EncryptedDisk, EncryptionKey, HEADER_LEN, MAGIC, SALT_LEN, Scan, VERSION, scan};
    use crate::{Disk, Rng};

    // ---- deterministic test doubles ----------------------------------
    //
    // Purely local test scaffolding (never `SimEnv`, never `OsRng`/
    // `thread_rng`) — mirrors the existing `CounterRng` precedent in
    // `s3_store.rs`'s own tests, but fixed rather than counter-seeded:
    // fixture *content* must be byte-identical on every run (ADR 0073's
    // "Determinism" convention), and a fixed salt is what makes that hold
    // here.

    /// An in-memory [`Disk`]: every method is synchronous under the hood
    /// (no real suspension across an `.await`), so [`block_on`] below
    /// resolves it on the first poll.
    #[derive(Default)]
    struct FixtureDisk {
        files: StdMutex<BTreeMap<String, Vec<u8>>>,
    }

    impl FixtureDisk {
        /// Seed `file` with raw bytes directly, bypassing any encryption —
        /// used to hand a fixture's already-sealed bytes to a fresh
        /// [`EncryptedDisk`] for decoding, without re-sealing them.
        fn seed(&self, file: &str, bytes: &[u8]) {
            self.files
                .lock()
                .expect("fixture disk lock poisoned")
                .insert(file.to_string(), bytes.to_vec());
        }
    }

    #[async_trait::async_trait]
    impl Disk for FixtureDisk {
        async fn append(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
            self.files
                .lock()
                .expect("fixture disk lock poisoned")
                .entry(file.to_string())
                .or_default()
                .extend_from_slice(bytes);
            Ok(())
        }

        async fn sync(&self, _file: &str) -> std::io::Result<()> {
            Ok(())
        }

        async fn read(&self, file: &str) -> std::io::Result<Vec<u8>> {
            Ok(self
                .files
                .lock()
                .expect("fixture disk lock poisoned")
                .get(file)
                .cloned()
                .unwrap_or_default())
        }

        async fn read_at(&self, file: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
            let data = self
                .files
                .lock()
                .expect("fixture disk lock poisoned")
                .get(file)
                .cloned()
                .unwrap_or_default();
            let offset = offset as usize;
            if offset >= data.len() {
                return Ok(Vec::new());
            }
            let end = offset.saturating_add(len).min(data.len());
            Ok(data[offset..end].to_vec())
        }

        async fn size(&self, file: &str) -> std::io::Result<u64> {
            Ok(self
                .files
                .lock()
                .expect("fixture disk lock poisoned")
                .get(file)
                .map(|v| v.len() as u64)
                .unwrap_or(0))
        }

        async fn remove(&self, file: &str) -> std::io::Result<()> {
            self.files
                .lock()
                .expect("fixture disk lock poisoned")
                .remove(file);
            Ok(())
        }

        async fn replace(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
            self.files
                .lock()
                .expect("fixture disk lock poisoned")
                .insert(file.to_string(), bytes.to_vec());
            Ok(())
        }

        async fn list(&self) -> std::io::Result<Vec<String>> {
            Ok(self
                .files
                .lock()
                .expect("fixture disk lock poisoned")
                .keys()
                .cloned()
                .collect())
        }

        async fn link(&self, src: &str, dst: &str) -> std::io::Result<()> {
            let data = self
                .files
                .lock()
                .expect("fixture disk lock poisoned")
                .get(src)
                .cloned()
                .unwrap_or_default();
            self.files
                .lock()
                .expect("fixture disk lock poisoned")
                .insert(dst.to_string(), data);
            Ok(())
        }
    }

    /// A fixed-byte [`Rng`]: `fill_bytes` always writes the ramp
    /// `0x40, 0x41, 0x42, ...`, so the salt this module's fixture/tests
    /// mint is the same 16 bytes on every run, on every contributor's
    /// machine, forever — never `OsRng`/`thread_rng`, and never `SimEnv`
    /// (this crate has no dependency on `animus-sim`).
    struct FixedTestRng;

    /// The exact salt [`FixedTestRng`] mints — spelled out once so both the
    /// generator and the decode assertions below refer to the same
    /// constant rather than re-deriving it.
    const FIXED_SALT: [u8; SALT_LEN] = [
        0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e,
        0x4f,
    ];

    impl Rng for FixedTestRng {
        fn next_u64(&self) -> u64 {
            0
        }

        fn fill_bytes(&self, dst: &mut [u8]) {
            for (i, b) in dst.iter_mut().enumerate() {
                *b = 0x40u8.wrapping_add(i as u8);
            }
        }
    }

    /// Poll `fut` to completion with a no-op waker. Every operation this
    /// module drives ([`FixtureDisk`]'s methods, `EncryptedDisk`'s own
    /// synchronous bookkeeping over them) never actually suspends across a
    /// real `.await`, so this always resolves on the first poll; it exists
    /// only so this test module needs no async-runtime dependency — `tokio`
    /// is gated behind this crate's `prod` feature (see `Cargo.toml`), and
    /// these tests must pass under a plain, feature-less `cargo test -p
    /// animus-env`.
    fn block_on<F: Future>(fut: F) -> F::Output {
        // `Waker::noop()` (stable, no `unsafe` needed — this crate's
        // workspace lints `forbid` unsafe code entirely) is exactly the
        // right waker here: nothing in this module ever actually parks,
        // so a wake notification is never needed.
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut fut = Box::pin(fut);
        loop {
            if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                return v;
            }
        }
    }

    /// A hand-chosen 256-bit constant — never a real secret, never `OsRng`
    /// — so the fixture and every test in this module encrypt/decrypt
    /// under the identical, reproducible key.
    const TEST_KEY: [u8; 32] = {
        let mut k = [0u8; 32];
        let mut i = 0;
        while i < 32 {
            k[i] = i as u8;
            i += 1;
        }
        k
    };

    fn test_key() -> EncryptionKey {
        EncryptionKey::from_bytes(TEST_KEY)
    }

    const FIXTURE_FILE_NAME: &str = "data";
    const PLAINTEXT_FRAME_1: &[u8] =
        b"AnimusDB ADE1 golden fixture, frame one (ADR 0073 Phase 0, Workstream A).";
    const PLAINTEXT_FRAME_2: &[u8] =
        b"Frame two: proves a multi-append (multi-frame) file decodes correctly too.";

    fn expected_plaintext() -> Vec<u8> {
        let mut expected = PLAINTEXT_FRAME_1.to_vec();
        expected.extend_from_slice(PLAINTEXT_FRAME_2);
        expected
    }

    fn fixtures_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/formats/encryption-envelope")
    }

    /// Build a fresh `EncryptedDisk` over the fixed key/salt test doubles,
    /// append the two representative frames, and return the *raw* (sealed)
    /// bytes the wrapped disk actually holds — the fixture's exact content.
    fn encode_representative_envelope() -> Vec<u8> {
        let disk = EncryptedDisk::new(FixtureDisk::default(), FixedTestRng, test_key());
        block_on(disk.append(FIXTURE_FILE_NAME, PLAINTEXT_FRAME_1)).expect("append frame 1");
        block_on(disk.append(FIXTURE_FILE_NAME, PLAINTEXT_FRAME_2)).expect("append frame 2");
        block_on(disk.inner().read(FIXTURE_FILE_NAME)).expect("read back raw sealed bytes")
    }

    /// Decode test (ADR 0073 Phase 0 convention): iterate every fixture
    /// file under `tests/fixtures/formats/encryption-envelope/`, decode
    /// each with the *current* code, and assert structurally — magic,
    /// version, salt, frame count, and full plaintext equality — rather
    /// than merely "decodes without error". Written to iterate the
    /// directory (not name `v1.bin` literally) so a future version bump
    /// needs no test-code change, only a new fixture file.
    #[test]
    fn decode_every_fixture_matches_current_code() {
        let dir = fixtures_dir();
        let mut checked = 0usize;
        for entry in std::fs::read_dir(&dir).expect("fixtures dir must exist") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("bin") {
                continue;
            }
            checked += 1;
            let raw = std::fs::read(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));

            // Header fields, parsed directly (mirrors what `scan` itself
            // checks first).
            assert_eq!(&raw[0..4], &MAGIC, "{path:?}: magic mismatch");
            let version = raw[4];
            assert!(
                version >= 1 && version <= VERSION,
                "{path:?}: version {version} outside the known range 1..={VERSION}"
            );

            let key = test_key();
            let index = match scan(&key, &raw) {
                Scan::Ok { index, torn } => {
                    assert!(
                        !torn,
                        "{path:?}: a checked-in fixture must not be a torn tail"
                    );
                    index
                }
                Scan::Absent => panic!("{path:?}: fixture must not decode as an absent/empty file"),
                Scan::UnsupportedVersion(v) => {
                    panic!("{path:?}: fixture has unsupported version {v}")
                }
                Scan::NotEncrypted => {
                    panic!("{path:?}: fixture failed the magic/header check under the test key")
                }
                Scan::Corrupted => {
                    panic!(
                        "{path:?}: fixture decoded as corrupted (mid-file tamper) — check-in is bad"
                    )
                }
            };

            // Full plaintext round trip through the public `Disk` API,
            // seeded directly with the fixture's already-sealed bytes (not
            // re-sealed) so this genuinely exercises decode, not encode.
            let fixture_disk = FixtureDisk::default();
            fixture_disk.seed(FIXTURE_FILE_NAME, &raw);
            let wrapped = EncryptedDisk::new(fixture_disk, FixedTestRng, test_key());
            let plaintext = block_on(wrapped.read(FIXTURE_FILE_NAME))
                .unwrap_or_else(|e| panic!("{path:?}: decrypt via EncryptedDisk::read: {e}"));

            // Expected value per fixture *version*: an unknown version has
            // no known salt/plaintext, so it panics rather than getting only
            // the weaker structural checks above (ADR 0073 Phase 1, P1-B).
            match version {
                1 => {
                    assert_eq!(
                        index.salt, FIXED_SALT,
                        "{path:?}: salt header field mismatch"
                    );
                    assert_eq!(index.frames.len(), 2, "{path:?}: expected exactly 2 frames");
                    assert_eq!(
                        index.frames[0].plain_len as usize,
                        PLAINTEXT_FRAME_1.len(),
                        "{path:?}: frame 0 plaintext length mismatch"
                    );
                    assert_eq!(
                        index.frames[1].plain_len as usize,
                        PLAINTEXT_FRAME_2.len(),
                        "{path:?}: frame 1 plaintext length mismatch"
                    );
                    assert_eq!(
                        plaintext,
                        expected_plaintext(),
                        "{path:?}: decrypted plaintext mismatch"
                    );
                }
                v => panic!(
                    "{path:?}: fixture is version {v} but this test has no expectation for it — \
                     add a per-version arm (ADR 0073 Phase 1 checklist)"
                ),
            }
            // The header byte must agree with the file name's version.
            assert_eq!(
                path.file_name().and_then(|n| n.to_str()),
                Some(format!("v{version}.bin").as_str()),
                "{path:?}: file name disagrees with the header version"
            );
        }
        assert!(checked > 0, "no fixture files found under {dir:?}");
        assert!(
            dir.join(format!("v{VERSION}.bin")).exists(),
            "no fixture for the current VERSION ({VERSION})"
        );
    }

    /// Round-trip test (ADR 0073 Phase 0 convention): encode a
    /// representative value with the *current* version, decode it back
    /// through the public `Disk` API, and assert equality — catches an
    /// encoder/decoder asymmetry a static fixture alone would miss.
    #[test]
    fn round_trip_current_version_encodes_and_decodes() {
        let disk = EncryptedDisk::new(FixtureDisk::default(), FixedTestRng, test_key());
        block_on(disk.append(FIXTURE_FILE_NAME, PLAINTEXT_FRAME_1)).expect("append frame 1");
        block_on(disk.append(FIXTURE_FILE_NAME, PLAINTEXT_FRAME_2)).expect("append frame 2");

        let round_tripped =
            block_on(disk.read(FIXTURE_FILE_NAME)).expect("read back through EncryptedDisk");
        assert_eq!(round_tripped, expected_plaintext());

        // The raw sealed bytes on the wire must carry today's magic/version.
        let raw = block_on(disk.inner().read(FIXTURE_FILE_NAME)).expect("read raw sealed bytes");
        assert_eq!(&raw[0..4], &MAGIC);
        assert_eq!(raw[4], VERSION);
    }

    /// Fixture generator (ADR 0073 Phase 0 convention): run explicitly via
    /// `cargo test -p animus-env --lib generate_fixture_encryption_envelope
    /// -- --ignored`. Refuses to overwrite a fixture that already exists —
    /// a format change is a version bump plus a new fixture file, never a
    /// rewrite of an old one.
    #[test]
    #[ignore]
    fn generate_fixture_encryption_envelope() {
        let path = fixtures_dir().join(format!("v{VERSION}.bin"));
        if std::fs::metadata(&path).is_ok() {
            panic!(
                "{path:?} already exists — a golden fixture is never overwritten once checked \
                 in (ADR 0073 Phase 0). If ADE1's on-disk shape genuinely changed, bump VERSION \
                 in encrypted.rs and add a new v{{N}}.bin fixture instead of regenerating this \
                 one."
            );
        }
        let raw = encode_representative_envelope();
        std::fs::create_dir_all(path.parent().expect("fixture path has a parent dir"))
            .expect("create fixtures directory");
        std::fs::write(&path, &raw).unwrap_or_else(|e| panic!("write {path:?}: {e}"));
    }

    /// Loud-error test: a wrong (or absent-from-a-too-short-header) magic
    /// must be a named `Err`, never a panic and never silently treated as
    /// plaintext. This already holds in today's code (`Scan::NotEncrypted`
    /// → `mixed_state_error`), so this pins that behavior as a regression
    /// guard rather than changing anything.
    #[test]
    fn wrong_magic_is_a_loud_error_never_a_panic() {
        let fixture_disk = FixtureDisk::default();
        fixture_disk.seed(
            FIXTURE_FILE_NAME,
            b"XXXX\x01................................................",
        );
        let wrapped = EncryptedDisk::new(fixture_disk, FixedTestRng, test_key());
        let err = block_on(wrapped.read(FIXTURE_FILE_NAME))
            .expect_err("a wrong magic must be a loud Err, never a panic or silent plaintext read");
        assert!(
            err.to_string()
                .contains("not in the expected encrypted frame format"),
            "unexpected error text: {err}"
        );
    }

    /// Loud-error test, the "too short to even hold a magic+salt header"
    /// variant of the same `Scan::NotEncrypted` path — also already a
    /// named `Err`, never a panic.
    #[test]
    fn header_too_short_for_a_magic_is_a_loud_error_never_a_panic() {
        let fixture_disk = FixtureDisk::default();
        fixture_disk.seed(FIXTURE_FILE_NAME, b"short");
        let wrapped = EncryptedDisk::new(fixture_disk, FixedTestRng, test_key());
        let err = block_on(wrapped.read(FIXTURE_FILE_NAME)).expect_err(
            "a too-short header must be a loud Err, never a panic or silent plaintext read",
        );
        assert!(
            err.to_string()
                .contains("not in the expected encrypted frame format"),
            "unexpected error text: {err}"
        );
    }

    fn assert_unsupported_version(version: u8) {
        let mut raw = encode_representative_envelope();
        raw[4] = version;
        let fixture_disk = FixtureDisk::default();
        fixture_disk.seed(FIXTURE_FILE_NAME, &raw);
        let wrapped = EncryptedDisk::new(fixture_disk, FixedTestRng, test_key());
        let err = block_on(wrapped.read(FIXTURE_FILE_NAME))
            .expect_err("an out-of-range ADE1 version must be a loud Err");
        let msg = err.to_string();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            msg.contains("unsupported ADE1 encryption envelope version")
                && msg.contains(&format!("version {version} "))
                && msg.contains(&format!("1..={VERSION}")),
            "unexpected error text: {msg}"
        );
    }

    /// ADR 0073 Phase 0: version 0 is loudly rejected, never decoded as v1.
    #[test]
    fn version_zero_is_a_loud_typed_error() {
        assert_unsupported_version(0);
    }

    /// ADR 0073 Phase 0: a future version is loudly rejected, never
    /// misread as v1 and never treated as torn/empty.
    #[test]
    fn future_version_is_a_loud_typed_error() {
        assert_unsupported_version(VERSION + 1);
        assert_unsupported_version(u8::MAX);
    }

    /// A strict prefix of the header (even one holding a bad version byte)
    /// classifies exactly as before the version check existed.
    #[test]
    fn torn_header_prefix_classification_unchanged() {
        let mut raw = encode_representative_envelope();
        raw[4] = VERSION + 1;
        for cut in [1, 4, 5, HEADER_LEN - 1] {
            assert!(
                matches!(scan(&test_key(), &raw[..cut]), Scan::NotEncrypted),
                "cut {cut}"
            );
        }
        assert!(matches!(scan(&test_key(), &[]), Scan::Absent));
        assert!(matches!(
            scan(&test_key(), &raw[..HEADER_LEN]),
            Scan::UnsupportedVersion(v) if v == VERSION + 1
        ));
    }
}
