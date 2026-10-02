//! Upgrade-restart harness, **tier 0: fixture-seeded restarts** (ADR 0073
//! Phase 1, workstream P1-D step 1).
//!
//! For each whole-file durable format the checked-in golden fixtures cover
//! (`lsm-wal`, `lsm-manifest`, `lsm-sstable`, `control-wal`, `shared-wal`),
//! seed a `SimEnv` disk with the fixture's bytes, run the disk through the
//! harness's per-format transcode table (`animus_test::upgrade::transcode`,
//! the identity today), crash the node, and open the **real** reader
//! (`LsmEngine::open`, `PersistedState` over `Disk::read`, `SharedWal::open`)
//! on it with the current code. The content read back must equal the
//! per-version expected value the fixture's own decode test pins.
//!
//! Needs no legacy encoder, so it exists from day one; tier 1 (workload-shaped
//! restarts over legacy-encoded state) builds on the same table. Fixtures are
//! only ever *read*; `scripts/check-format-fixtures.sh` guards them.
//!
//! Seed-reproducible: every scenario prints its seed in assertion messages and
//! `ANIMUS_SEED=<seed>` replays exactly that one. The seed varies the node id,
//! the file-name prefix, and whether a crash sits between seeding and opening.
//!
//! **Negative controls** give the tier teeth: a deliberately corrupting
//! transcode (zeroed version tag, a flipped record byte, a forged future tag,
//! a truncated file) must be caught, and each control asserts the *precise*
//! outcome. A torn tail is tolerated by recovery by design, so the controls
//! that hit a tolerated shape assert that the open succeeds but the content
//! check fails; the others assert the named error.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use animus_control::format::{FormatTag, encode_line};
use animus_control::persist::{CONTROL_WAL, PersistedState, SHARED_WAL_TAG, WalRecord};
use animus_control::raft::LogEntry;
use animus_control::shared_wal::SharedWal;
use animus_control::{MetaCommand, Metadata, NodeStatus};
use animus_env::{Disk, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, MergeOp, StorageEngine, StorageError};
use animus_tablet::TabletId;
use animus_test::corpus::{for_each_seed, name_seed, seeds_from_env};
use animus_test::upgrade::transcode::{self, TranscodeOpts};
use futures::executor::block_on;

// ---------------------------------------------------------------------------
// Fixtures and seeding

/// Every crate's `tests/fixtures/formats` directory.
fn format_roots() -> Vec<PathBuf> {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut roots: Vec<PathBuf> = std::fs::read_dir(&crates)
        .expect("read crates/")
        .map(|e| e.expect("dir entry").path().join("tests/fixtures/formats"))
        .filter(|p| p.is_dir())
        .collect();
    roots.sort();
    roots
}

/// The fixture directory of `format`, found by scanning every crate (so a
/// table entry needs no crate bookkeeping).
fn fixture_dir(format: &str) -> PathBuf {
    let hits: Vec<PathBuf> = format_roots()
        .into_iter()
        .map(|r| r.join(format))
        .filter(|p| p.is_dir())
        .collect();
    assert_eq!(
        hits.len(),
        1,
        "{format}: expected one fixture dir, got {hits:?}"
    );
    hits.into_iter().next().unwrap()
}

/// Every checked-in fixture of `format`, keyed by the version in its file name.
fn fixtures(format: &str) -> BTreeMap<u32, Vec<u8>> {
    let dir = fixture_dir(format);
    let mut out = BTreeMap::new();
    for entry in std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
        let path = entry.expect("dir entry").path();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        let version: u32 = name
            .strip_prefix('v')
            .and_then(|s| s.strip_suffix(".bin"))
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| panic!("{format}: unexpected fixture file name {name:?}"));
        out.insert(version, std::fs::read(&path).expect("read fixture"));
    }
    assert!(!out.is_empty(), "no fixtures under {}", dir.display());
    out
}

/// One seeded scenario's disk: which node, which file-name prefix, and whether
/// a crash (power loss, dropping volatile state) separates seeding from open.
struct Scenario {
    seed: u64,
    sim: Simulator,
    node: NodeId,
    prefix: &'static str,
    crash_first: bool,
}

impl Scenario {
    fn new(seed: u64) -> Self {
        let pick = |salt: &str| name_seed(&format!("{salt}/{seed}"));
        let prefixes = ["", "lsm/", "t9-"];
        Self {
            seed,
            sim: Simulator::new(seed),
            node: nid(pick("node") % 3),
            prefix: prefixes[(pick("prefix") % prefixes.len() as u64) as usize],
            crash_first: pick("crash") % 2 == 0,
        }
    }

    fn env(&self) -> SimEnv {
        self.sim.env(self.node.clone())
    }

    /// Write `bytes` durably as `{prefix}{name}`; returns the full file name.
    fn seed_file(&self, name: &str, bytes: &[u8]) -> String {
        let file = format!("{}{name}", self.prefix);
        block_on(self.env().replace(&file, bytes)).expect("seed file");
        file
    }

    /// The harness step between "stop the node" and "restart": the table
    /// transcode back zero versions (identity today), then an optional crash.
    fn transcode_and_stop(&self) {
        let before = self.snapshot_disk();
        let report = transcode::transcode_disk(&self.env(), 0, &TranscodeOpts::default())
            .unwrap_or_else(|e| panic!("seed={}: transcode_disk: {e}", self.seed));
        assert!(
            !report.transcoded.is_empty() || !report.unrecognised.is_empty(),
            "seed={}: the pass saw no files",
            self.seed
        );
        assert_eq!(
            before,
            self.snapshot_disk(),
            "seed={}: the v1 identity transcode must leave every byte unchanged",
            self.seed
        );
        if self.crash_first {
            self.sim.crash(self.node.clone());
        }
    }

    fn snapshot_disk(&self) -> BTreeMap<String, Vec<u8>> {
        let env = self.env();
        block_on(async {
            let mut out = BTreeMap::new();
            for f in env.list().await.expect("list") {
                out.insert(f.clone(), env.read(&f).await.expect("read"));
            }
            out
        })
    }
}

/// Run `body` for every seed of `name` (or the single `ANIMUS_SEED`).
fn each_seed(name: &str, mut body: impl FnMut(u64)) {
    if let Some(seed) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        eprintln!("{name}: replaying ANIMUS_SEED={seed}");
        body(seed);
        return;
    }
    // Tier 0 is cheap; default to a few seeds so prefix/node/crash variants
    // are all covered, and let the depth knob widen it.
    let k = seeds_from_env("ANIMUS_UPGRADE_RESTART_SEEDS").max(6);
    for_each_seed(name, k, |seed| {
        eprintln!("{name}: seed={seed}");
        body(seed);
    });
}

// ---------------------------------------------------------------------------
// The table must not drift from the fixtures

/// Checklist step 7 made mechanical: every fixture version has a table entry
/// version, and the table's current version is the newest fixture's. A format
/// that bumped without registering its pair fails here. Iterates the whole
/// `TABLE` (a whole-file format with no fixture is itself a failure).
#[test]
fn transcode_table_matches_the_checked_in_fixtures() {
    for entry in transcode::TABLE {
        let format = entry.name;
        let versions: Vec<u32> = fixtures(format).keys().copied().collect();
        assert_eq!(
            entry.current_version,
            *versions.last().unwrap(),
            "{format}: TABLE current_version differs from the newest fixture \
             (checklist step 7: register the new pair)"
        );
        for v in versions {
            assert!(
                entry.capabilities(v).is_some(),
                "{format}: fixture v{v} has no VersionSpec in TABLE"
            );
        }
    }
    for e in transcode::EMBEDDED {
        let newest = newest_fixture_version(&fixture_dir(e.name));
        assert_eq!(
            e.current_version, newest,
            "{}: EMBEDDED current_version differs from the newest fixture \
             (a bump edits the carrier's transcode and this row)",
            e.name
        );
    }
}

/// The newest `v<N>.<ext>` fixture version under `dir`, recursing into
/// subdirectories (`mirror-entities` has one directory per entity kind; the
/// newest version must be common to every kind's newest, so take the minimum
/// of per-directory maxima).
fn newest_fixture_version(dir: &Path) -> u32 {
    let mut here: Option<u32> = None;
    let mut sub: Option<u32> = None;
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            let v = newest_fixture_version(&path);
            sub = Some(sub.map_or(v, |s| s.min(v)));
            continue;
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        let v: u32 = name
            .strip_prefix('v')
            .and_then(|s| s.split('.').next())
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                panic!("unexpected fixture file name {name:?} in {}", dir.display())
            });
        here = Some(here.map_or(v, |h| h.max(v)));
    }
    here.or(sub)
        .unwrap_or_else(|| panic!("no fixtures under {}", dir.display()))
}

/// The names in `dirs` that neither `TABLE` nor `EMBEDDED` registers. Pure, so
/// the negative control can feed it a synthetic directory.
fn unregistered(dirs: &[String]) -> Vec<String> {
    dirs.iter()
        .filter(|d| {
            transcode::entry(d).is_none() && !transcode::EMBEDDED.iter().any(|e| e.name == **d)
        })
        .cloned()
        .collect()
}

fn all_fixture_dir_names() -> Vec<String> {
    let mut names = Vec::new();
    for root in format_roots() {
        for e in std::fs::read_dir(&root).expect("read formats dir") {
            let p = e.expect("dir entry").path();
            if p.is_dir() {
                names.push(p.file_name().unwrap().to_string_lossy().into_owned());
            }
        }
    }
    names.sort();
    names
}

/// Every format fixture directory is accounted for by the harness, every
/// registration names a real directory, and every embedded carrier is a real
/// `TABLE` entry.
#[test]
fn every_fixture_dir_is_registered_with_the_harness() {
    let dirs = all_fixture_dir_names();
    assert_eq!(
        unregistered(&dirs),
        Vec::<String>::new(),
        "fixture dir(s) in no TABLE/EMBEDDED entry (ADR 0073 step 7): register \
         whole-file formats in transcode::TABLE, others in transcode::EMBEDDED"
    );
    let mut registered: Vec<&str> = transcode::TABLE.iter().map(|e| e.name).collect();
    registered.extend(transcode::EMBEDDED.iter().map(|e| e.name));
    let n = registered.len();
    registered.sort_unstable();
    registered.dedup();
    assert_eq!(n, registered.len(), "a format is registered twice");
    for name in registered {
        assert!(
            dirs.iter().any(|d| d == name),
            "{name}: registered but no tests/fixtures/formats/{name} directory"
        );
    }
    for e in transcode::EMBEDDED {
        if let transcode::Carrier::Table(t) = e.carrier {
            assert!(
                transcode::entry(t).is_some(),
                "{}: carrier {t:?} is not a TABLE entry",
                e.name
            );
        }
    }
}

/// Negative control: the completeness check has teeth. A synthetic unregistered
/// directory is reported, and registered ones are not.
#[test]
fn completeness_check_flags_an_unregistered_fixture_dir() {
    let mut dirs = all_fixture_dir_names();
    assert!(unregistered(&dirs).is_empty());
    dirs.push("brand-new-format".to_string());
    assert_eq!(unregistered(&dirs), vec!["brand-new-format".to_string()]);
}

// ---------------------------------------------------------------------------
// lsm-wal

fn lsm_opts() -> LsmOptions {
    LsmOptions {
        background_maintenance: false,
        ..LsmOptions::default()
    }
}

type Entries = Vec<(Vec<u8>, Option<Vec<u8>>, u64)>;

/// The exact visible content of the `lsm-wal` fixture after replay, derived by
/// hand from `representative_records()` in `animus-storage/src/lsm.rs`:
/// `Put`, `Delete`, `DeleteRange` (tombstones its named keys), a `Batch`
/// (put, delete, delete-keys) and a `MergeBatch` (value + tombstone).
fn lsm_wal_expected(version: u32) -> (Entries, u64) {
    match version {
        1 => {
            let put =
                |k: &str, v: &str, ver| (k.as_bytes().to_vec(), Some(v.as_bytes().to_vec()), ver);
            let del = |k: &str, ver| (k.as_bytes().to_vec(), None, ver);
            (
                vec![
                    del("batch-delete", 4),
                    del("batch-key-a", 4),
                    del("batch-key-b", 4),
                    put("batch-put", "batch-value", 4),
                    del("key-delete", 2),
                    put("key-put", "value-put", 1),
                    put("merge-a", "merge-value-a", 5),
                    del("merge-b", 6),
                    del("range-key-a", 3),
                    del("range-key-b", 3),
                ],
                6,
            )
        }
        v => panic!("lsm-wal v{v} fixture has no tier-0 expectation yet — add an arm"),
    }
}

/// Open an `LsmEngine` on the scenario's disk and read everything back.
fn read_lsm(sc: &Scenario) -> Result<(Entries, u64), StorageError> {
    block_on(async {
        let e = LsmEngine::open_with(sc.env(), sc.prefix, lsm_opts()).await?;
        Ok((e.entries_with_tombstones().await?, e.latest_version()))
    })
}

fn lsm_wal_run(seed: u64, bytes: &[u8]) -> (Scenario, Result<(Entries, u64), StorageError>) {
    let sc = Scenario::new(seed);
    sc.seed_file("wal-000000", bytes);
    sc.transcode_and_stop();
    let got = read_lsm(&sc);
    (sc, got)
}

#[test]
fn lsm_wal_fixture_restarts_with_current_code() {
    for (version, bytes) in fixtures("lsm-wal") {
        each_seed("tier0_lsm_wal", |seed| {
            let (sc, got) = lsm_wal_run(seed, &bytes);
            let got = got.unwrap_or_else(|e| panic!("seed={seed}: v{version} open failed: {e}"));
            assert_eq!(
                got,
                lsm_wal_expected(version),
                "seed={seed}: v{version} content"
            );

            // The restarted engine keeps accepting writes and they survive
            // another restart (rewritten in the current version).
            let env = sc.env();
            block_on(async {
                let e = LsmEngine::open_with(env.clone(), sc.prefix, lsm_opts())
                    .await
                    .unwrap();
                e.put(b"post-upgrade", b"w", 7).await.unwrap();
            });
            sc.sim.crash(sc.node.clone());
            let (entries, max) =
                read_lsm(&sc).unwrap_or_else(|e| panic!("seed={seed}: reopen: {e}"));
            assert_eq!(max, 7, "seed={seed}");
            assert!(
                entries.contains(&(b"post-upgrade".to_vec(), Some(b"w".to_vec()), 7)),
                "seed={seed}: post-upgrade write lost"
            );
        });
    }
}

/// Negative controls for `lsm-wal`. A zeroed version byte must be a named
/// error. A flipped byte inside the first record, with valid records after it,
/// is not a torn tail: recovery refuses it loudly rather than drop history. A
/// truncated tail *is* tolerated by design, so there the open succeeds and the
/// content check (the tier's teeth) is what must fail.
#[test]
fn lsm_wal_negative_controls_are_caught() {
    let v1 = fixtures("lsm-wal")[&1].clone();
    each_seed("tier0_lsm_wal_neg", |seed| {
        let mut zero_version = v1.clone();
        zero_version[4] = 0;
        match lsm_wal_run(seed, &zero_version).1 {
            Err(StorageError::UnsupportedFormatVersion {
                format: "lsm-wal",
                found: 0,
                ..
            }) => {}
            other => panic!(
                "seed={seed}: zeroed version tag: expected UnsupportedFormatVersion, got {other:?}"
            ),
        }

        let mut flipped = v1.clone();
        flipped[5 + 12] ^= 0xff; // inside the first record's frame
        match lsm_wal_run(seed, &flipped).1 {
            Err(StorageError::Backend(m)) if m.contains("not a torn tail") => {}
            other => {
                panic!("seed={seed}: mid-file corruption must be refused as such, got {other:?}")
            }
        }

        let truncated = &v1[..v1.len() - 9];
        let got = lsm_wal_run(seed, truncated)
            .1
            .unwrap_or_else(|e| panic!("seed={seed}: a truncated tail is tolerated, got {e}"));
        assert_ne!(
            got,
            lsm_wal_expected(1),
            "seed={seed}: truncation went unnoticed"
        );
    });
}

// ---------------------------------------------------------------------------
// lsm-manifest

/// The manifest fixture names tables `sst-000001..3` whose metadata is
/// synthetic, so no engine can be opened *fully* from it. What it can prove is
/// that the real `decode_manifest` accepts the fixture and recovery then acts
/// on its table list: the first thing `open` does after decoding is read the
/// first table's index region (`sst-000001`, absent), so the error must be the
/// sstable-index read failing and never a manifest/format error. (The sstable's own tier-0 test below opens a full engine.)
fn lsm_manifest_run(seed: u64, bytes: &[u8]) -> Result<(), StorageError> {
    let sc = Scenario::new(seed);
    sc.seed_file("MANIFEST", bytes);
    sc.transcode_and_stop();
    match block_on(LsmEngine::open_with(sc.env(), sc.prefix, lsm_opts())) {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

#[test]
fn lsm_manifest_fixture_decodes_and_recovery_proceeds_to_its_tables() {
    for (version, bytes) in fixtures("lsm-manifest") {
        match version {
            1 => {}
            v => panic!("lsm-manifest v{v} fixture has no tier-0 expectation yet — add an arm"),
        }
        each_seed("tier0_lsm_manifest", |seed| {
            let err = lsm_manifest_run(seed, &bytes)
                .expect_err("the manifest's tables do not exist, so open must stop there");
            let msg = err.to_string();
            assert!(
                matches!(err, StorageError::Backend(_)) && msg.contains("sstable index"),
                "seed={seed}: expected the open to get past the manifest decode and fail \
                 reading the first table's index, got: {msg}"
            );
        });
    }
}

#[test]
fn lsm_manifest_negative_controls_are_caught() {
    let v1 = fixtures("lsm-manifest")[&1].clone();
    each_seed("tier0_lsm_manifest_neg", |seed| {
        let mut zero_version = v1.clone();
        zero_version[4] = 0;
        match lsm_manifest_run(seed, &zero_version) {
            Err(StorageError::UnsupportedFormatVersion {
                format: "lsm-manifest",
                found: 0,
                ..
            }) => {}
            other => panic!(
                "seed={seed}: zeroed version: expected UnsupportedFormatVersion, got {other:?}"
            ),
        }
        let mut no_magic = v1.clone();
        no_magic[0] = b'X';
        match lsm_manifest_run(seed, &no_magic) {
            Err(StorageError::PreBaselineFormat {
                format: "lsm-manifest",
            }) => {}
            other => panic!("seed={seed}: bad magic: expected PreBaselineFormat, got {other:?}"),
        }
        match lsm_manifest_run(seed, &v1[..v1.len() - 7]) {
            Err(StorageError::Backend(m))
                if m.contains("manifest") || m.contains("crc") || m.contains("truncated") => {}
            other => {
                panic!("seed={seed}: truncated manifest must be a decode error, got {other:?}")
            }
        }
    });
}

// ---------------------------------------------------------------------------
// lsm-sstable

/// The 300 records behind the `lsm-sstable` fixture: `fixture_records()` in
/// `animus-storage/src/lsm/sstable.rs`, reproduced here (the fixture's own
/// test helper is private). The expected value is this list, in key order.
fn sstable_expected(version: u32) -> Entries {
    match version {
        1 => {
            let mut state = 0x0123_4567_89ab_cdefu64;
            let mut out = Vec::new();
            for i in 0u32..300 {
                let key = format!("fixture-key-{i:04}").into_bytes();
                let ver = u64::from(i % 5) + 1;
                let value = if i < 100 {
                    Some(vec![b'A' + (i % 3) as u8; 64])
                } else if i < 200 {
                    let mut v = Vec::with_capacity(48);
                    for _ in 0..48 {
                        state = state
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        v.push((state >> 33) as u8);
                    }
                    Some(v)
                } else if i % 3 == 0 {
                    None
                } else if i % 3 == 1 {
                    Some(Vec::new())
                } else {
                    Some(format!("value-{i}").into_bytes())
                };
                out.push((key, value, ver));
            }
            out
        }
        v => panic!("lsm-sstable v{v} fixture has no tier-0 expectation yet — add an arm"),
    }
}

/// A manifest describing exactly one table with the fixture's shape, obtained
/// by flushing the same records through a scratch engine (the manifest codec
/// is private, and the fixture manifest describes different, synthetic
/// tables). Returns `(manifest bytes, table file name, scratch table bytes)`.
fn scratch_manifest_for_sstable(version: u32) -> (Vec<u8>, String, Vec<u8>) {
    let scratch = Simulator::new(1);
    let env = scratch.env(nid(0));
    block_on(async {
        let e = LsmEngine::open_with(env.clone(), "", lsm_opts())
            .await
            .unwrap();
        let ops = sstable_expected(version)
            .into_iter()
            .map(|(k, v, ver)| MergeOp {
                key: k,
                value: v,
                version: ver,
            })
            .collect();
        e.merge_batch(ops).await.unwrap();
        e.flush_now().await.unwrap();
        assert_eq!(e.sstable_count(), 1, "scratch flush must produce one table");
        let files = env.list().await.unwrap();
        let sst = files
            .iter()
            .find(|f| f.starts_with("sst-"))
            .cloned()
            .expect("an sstable");
        (
            env.read("MANIFEST").await.unwrap(),
            sst.clone(),
            env.read(&sst).await.unwrap(),
        )
    })
}

fn sstable_run(seed: u64, version: u32, table: &[u8]) -> Result<Entries, StorageError> {
    let (manifest, sst_name, scratch_table) = scratch_manifest_for_sstable(version);
    // The manifest records the table's index offset/length/size; a fixture
    // whose footer disagrees would fail in a confusing way, so say why.
    let footer = |b: &[u8]| b[b.len().saturating_sub(24)..].to_vec();
    assert_eq!(
        (table.len(), footer(table)),
        (scratch_table.len(), footer(&scratch_table)),
        "tier 0 derives the manifest from a current-writer flush; when the \
         sstable layout changes, give this test a per-version manifest"
    );
    let sc = Scenario::new(seed);
    sc.seed_file("MANIFEST", &manifest);
    sc.seed_file(&sst_name, table);
    sc.transcode_and_stop();
    block_on(async {
        let e = LsmEngine::open_with(sc.env(), sc.prefix, lsm_opts()).await?;
        e.entries_with_tombstones().await
    })
}

#[test]
fn lsm_sstable_fixture_restarts_with_current_code() {
    for (version, bytes) in fixtures("lsm-sstable") {
        each_seed("tier0_lsm_sstable", |seed| {
            let got = sstable_run(seed, version, &bytes)
                .unwrap_or_else(|e| panic!("seed={seed}: v{version} open/read failed: {e}"));
            assert_eq!(
                got,
                sstable_expected(version),
                "seed={seed}: v{version} content"
            );
        });
    }
}

/// A flipped byte inside a data block is covered by the block CRC: reading it
/// back must fail loudly (a table is reachable only after its synced manifest
/// swap, so a torn block is never a legitimate recovery shape).
#[test]
fn lsm_sstable_negative_controls_are_caught() {
    let v1 = fixtures("lsm-sstable")[&1].clone();
    each_seed("tier0_lsm_sstable_neg", |seed| {
        let mut flipped = v1.clone();
        flipped[40] ^= 0xff; // inside the first data block
        match sstable_run(seed, 1, &flipped) {
            Err(StorageError::Backend(m)) => {
                assert!(
                    m.contains("crc") || m.contains("corrupt"),
                    "seed={seed}: {m}"
                )
            }
            other => panic!("seed={seed}: flipped block byte must fail the read, got {other:?}"),
        }
    });
}

// ---------------------------------------------------------------------------
// control-wal (`CWL1`) and shared-wal (`SWL1`)

/// Mirrors the records the fixture tests in
/// `animus-control/tests/format_fixtures.rs` pin (one of each `WalRecord`).
fn control_wal_expected(version: u32) -> Vec<WalRecord<MetaCommand, Metadata>> {
    match version {
        // v2 adds only sync-marker lines, which decoding consumes.
        1 | 2 => vec![
            WalRecord::Hard {
                term: 3,
                voted_for: Some(nid(1)),
            },
            WalRecord::Append(upsert_entry(1, 3, 2)),
            WalRecord::Truncate { keep: 1 },
            WalRecord::Snapshot {
                metadata: Metadata::default(),
                last_index: 5,
                last_term: 3,
                config: Some(BTreeSet::from([nid(1), nid(2)])),
                learners: Some(BTreeSet::from([nid(3)])),
            },
        ],
        v => panic!("control-wal v{v} fixture has no tier-0 expectation yet — add an arm"),
    }
}

fn upsert_entry(index: u64, term: u64, node: u64) -> LogEntry<MetaCommand> {
    LogEntry {
        index,
        term,
        command: MetaCommand::UpsertMember {
            node: nid(node),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        },
        config: None,
        learners: None,
    }
}

type ControlRecords = Vec<WalRecord<MetaCommand, Metadata>>;

/// The control plane's recovery read, exactly as `node::drive` does it:
/// `env.read("raft.wal")`, `PersistedState::decode`, then `replay`.
fn control_wal_run(seed: u64, bytes: &[u8]) -> Result<(ControlRecords, PersistedState), String> {
    let sc = Scenario::new(seed);
    // The real file name; the prefix variant is not applicable to a fixed name.
    block_on(sc.env().replace("raft.wal", bytes)).expect("seed");
    let report =
        transcode::transcode_disk(&sc.env(), 0, &TranscodeOpts::default()).expect("transcode");
    assert_eq!(report.transcoded.len(), 1, "seed={seed}: {report:?}");
    assert_eq!(report.transcoded[0].1, "control-wal");
    if sc.crash_first {
        sc.sim.crash(sc.node.clone());
    }
    let read = block_on(sc.env().read("raft.wal")).expect("read");
    let records =
        PersistedState::<MetaCommand, Metadata>::decode(&read).map_err(|e| e.to_string())?;
    let state = PersistedState::replay(records.clone());
    Ok((records, state))
}

#[test]
fn control_wal_fixture_restarts_with_current_code() {
    for (version, bytes) in fixtures("control-wal") {
        each_seed("tier0_control_wal", |seed| {
            let (records, state) = control_wal_run(seed, &bytes)
                .unwrap_or_else(|e| panic!("seed={seed}: v{version}: {e}"));
            assert_eq!(records, control_wal_expected(version), "seed={seed}");
            // The replayed Raft state recovery would hand to `RaftCore::recovered`.
            assert_eq!(state.term, 3, "seed={seed}");
            assert_eq!(state.voted_for, Some(nid(1)), "seed={seed}");
            assert_eq!(
                state.snapshot.as_ref().map(|s| (s.1, s.2)),
                Some((5, 3)),
                "seed={seed}"
            );
            assert_eq!(
                state.snapshot_config,
                Some(BTreeSet::from([nid(1), nid(2)])),
                "seed={seed}"
            );
            assert_eq!(
                state.snapshot_learners,
                Some(BTreeSet::from([nid(3)])),
                "seed={seed}"
            );
            assert_eq!(state.log, vec![upsert_entry(1, 3, 2)], "seed={seed}");
        });
    }
}

/// The v2 -> v1 transcode (ADR 0073 coordination note): the legacy reframer
/// drops the sync-marker lines and re-frames every record under the v1 tag, so
/// the result is a marker-less v1 file that decodes to the same records.
#[test]
fn control_wal_v2_transcodes_to_a_v1_file_with_the_same_records() {
    let entry = transcode::TABLE
        .iter()
        .find(|e| e.name == "control-wal")
        .expect("control-wal entry");
    let v2 = fixtures("control-wal")[&2].clone();
    let v1 = entry.transcode_to(&v2, 1).expect("v2 -> v1");
    assert!(
        !v1.windows(6).any(|w| w == b"!sync:"),
        "v1 has no marker lines"
    );
    assert!(
        v1.split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .all(|l| &l[9..13] == b"CWL1" && &l[13..15] == b"01"),
        "every line re-framed under the v1 tag"
    );
    assert_eq!(
        PersistedState::<MetaCommand, Metadata>::decode(&v1).expect("v1 decodes"),
        control_wal_expected(1)
    );
    // (Not byte-equal to the v1 fixture: its `Snapshot` line embeds a
    // `Metadata` serialized before `Metadata` grew its `"v"` field; the
    // records are what must match.)
}

/// `CWL1` lines are CRC-framed, so zeroing the version *in place* breaks the
/// CRC and reads as a torn line (tolerated: content check fails). A forged
/// line with a valid CRC but a future version is the named-error shape.
#[test]
fn control_wal_negative_controls_are_caught() {
    let v1 = fixtures("control-wal")[&1].clone();
    each_seed("tier0_control_wal_neg", |seed| {
        let mut flipped = v1.clone();
        flipped[20] ^= 0xff; // inside the first record's payload
        // v1-ONLY behaviour: a v1 file has no sync markers, so a CRC failure on a
        // line is treated as a torn tail *wherever* it sits (decode succeeds and
        // drops the line and everything after it). A v2 file refuses the same
        // damage; see the v2 counterpart below. Pin the v1 outcome: the open is
        // fine, the content is not.
        let (records, _) = control_wal_run(seed, &flipped).unwrap_or_else(|e| {
            panic!("seed={seed}: a CRC-failed line reads as a torn tail, got {e}")
        });
        assert_ne!(
            records,
            control_wal_expected(1),
            "seed={seed}: corruption unnoticed"
        );

        // v2 counterpart (issue #1132): the same first-line damage, with a
        // durable sync marker after it, is the named mid-file corruption error.
        let v2 = fixtures("control-wal")[&2].clone();
        let mut flipped2 = v2.clone();
        flipped2[20] ^= 0xff;
        let err = control_wal_run(seed, &flipped2)
            .expect_err("a corrupted first v2 line before a marker must be refused");
        assert!(err.contains("corrupt"), "seed={seed}: {err}");

        let future = encode_line(
            &FormatTag {
                version: CONTROL_WAL.version + 1,
                ..CONTROL_WAL
            },
            b"{}",
        );
        let err =
            control_wal_run(seed, &future).expect_err("a future CWL1 version must be refused");
        assert!(err.contains("version"), "seed={seed}: {err}");

        let truncated = &v1[..v1.len() - 40];
        let (records, _) = control_wal_run(seed, truncated)
            .unwrap_or_else(|e| panic!("seed={seed}: truncated tail is tolerated, got {e}"));
        assert_ne!(
            records,
            control_wal_expected(1),
            "seed={seed}: truncation unnoticed"
        );
    });
}

fn shared_wal_expected(version: u32) -> Vec<(TabletId, WalRecord<MetaCommand, Metadata>)> {
    match version {
        // v2 adds only sync-marker lines, which decoding consumes.
        1 | 2 => {
            let entry = |i, t, n| WalRecord::Append(upsert_entry(i, t, n));
            vec![
                (
                    TabletId(1),
                    WalRecord::Hard {
                        term: 2,
                        voted_for: Some(nid(1)),
                    },
                ),
                (
                    TabletId(2),
                    WalRecord::Hard {
                        term: 4,
                        voted_for: None,
                    },
                ),
                (TabletId(1), entry(1, 2, 2)),
                (TabletId(7), entry(1, 4, 3)),
                (TabletId(2), entry(1, 4, 4)),
                (TabletId(1), entry(2, 2, 5)),
                (TabletId(1), WalRecord::Truncate { keep: 1 }),
                (
                    TabletId(7),
                    WalRecord::Snapshot {
                        metadata: Metadata::default(),
                        last_index: 5,
                        last_term: 4,
                        config: Some(BTreeSet::from([nid(1), nid(2)])),
                        learners: Some(BTreeSet::from([nid(3)])),
                    },
                ),
                (TabletId(2), entry(2, 4, 6)),
            ]
        }
        v => panic!("shared-wal v{v} fixture has no tier-0 expectation yet — add an arm"),
    }
}

/// `SharedWal::open` (the real recovery seeding read) then each tablet's
/// `recovered_state`, returned as comparable per-tablet summaries.
#[allow(clippy::type_complexity)]
fn shared_wal_run(
    seed: u64,
    bytes: &[u8],
) -> std::io::Result<BTreeMap<TabletId, (u64, Option<NodeId>, Vec<LogEntry<MetaCommand>>)>> {
    let sc = Scenario::new(seed);
    let file = "raftkv.wal.shared";
    block_on(sc.env().replace(file, bytes)).expect("seed");
    let report =
        transcode::transcode_disk(&sc.env(), 0, &TranscodeOpts::default()).expect("transcode");
    assert_eq!(report.transcoded.len(), 1, "seed={seed}: {report:?}");
    assert_eq!(report.transcoded[0].1, "shared-wal");
    if sc.crash_first {
        sc.sim.crash(sc.node.clone());
    }
    let env = sc.env();
    block_on(async {
        let wal = SharedWal::<MetaCommand, Metadata>::open(&env, file).await?;
        let mut out = BTreeMap::new();
        for t in [1u64, 2, 7, 99] {
            let s = wal.recovered_state(TabletId(t)).await;
            out.insert(TabletId(t), (s.term, s.voted_for, s.log));
        }
        Ok(out)
    })
}

/// The per-tablet summary replaying the expected lines yields.
#[allow(clippy::type_complexity)]
fn shared_wal_expected_state(
    version: u32,
) -> BTreeMap<TabletId, (u64, Option<NodeId>, Vec<LogEntry<MetaCommand>>)> {
    let lines = shared_wal_expected(version);
    let mut out = BTreeMap::new();
    for t in [1u64, 2, 7, 99] {
        let recs = lines
            .iter()
            .filter(|(id, _)| id.0 == t)
            .map(|(_, r)| r.clone());
        let s = PersistedState::<MetaCommand, Metadata>::replay(recs);
        out.insert(TabletId(t), (s.term, s.voted_for, s.log));
    }
    out
}

#[test]
fn shared_wal_fixture_restarts_with_current_code() {
    for (version, bytes) in fixtures("shared-wal") {
        each_seed("tier0_shared_wal", |seed| {
            let got = shared_wal_run(seed, &bytes)
                .unwrap_or_else(|e| panic!("seed={seed}: v{version} open failed: {e}"));
            assert_eq!(
                got,
                shared_wal_expected_state(version),
                "seed={seed}: per-tablet recovery"
            );
            // Tablet 99 never appears in the file: a fresh group.
            assert_eq!(got[&TabletId(99)], (0, None, Vec::new()), "seed={seed}");
        });
    }
}

#[test]
fn shared_wal_negative_controls_are_caught() {
    let v1 = fixtures("shared-wal")[&1].clone();
    each_seed("tier0_shared_wal_neg", |seed| {
        let mut flipped = v1.clone();
        flipped[20] ^= 0xff;
        // v1-ONLY behaviour: no sync markers, so a CRC failure anywhere reads
        // as a torn tail. The v2 counterpart follows.
        let got = shared_wal_run(seed, &flipped).unwrap_or_else(|e| {
            panic!("seed={seed}: a CRC-failed line reads as a torn tail, got {e}")
        });
        assert_ne!(
            got,
            shared_wal_expected_state(1),
            "seed={seed}: corruption unnoticed"
        );

        // v2 counterpart (issue #1132): the same first-line damage before a
        // durable marker fails the open with InvalidData.
        let mut flipped2 = fixtures("shared-wal")[&2].clone();
        flipped2[20] ^= 0xff;
        let err = shared_wal_run(seed, &flipped2)
            .expect_err("a corrupted first v2 line before a marker must fail the open");
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::InvalidData,
            "seed={seed}: {err}"
        );
        assert!(err.to_string().contains("corrupt"), "seed={seed}: {err}");

        let future = encode_line(
            &FormatTag {
                version: SHARED_WAL_TAG.version + 1,
                ..SHARED_WAL_TAG
            },
            b"{}",
        );
        let err =
            shared_wal_run(seed, &future).expect_err("a future SWL1 version must fail the open");
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::InvalidData,
            "seed={seed}: {err}"
        );

        let got = shared_wal_run(seed, &v1[..v1.len() - 50])
            .unwrap_or_else(|e| panic!("seed={seed}: truncated tail is tolerated, got {e}"));
        assert_ne!(
            got,
            shared_wal_expected_state(1),
            "seed={seed}: truncation unnoticed"
        );
    });
}

// ---------------------------------------------------------------------------
// The disk-level entry point itself

/// `transcode_disk` over a mixed node disk: sorted, classified, every
/// unsupported request refused, and the seeded keep/skip decision is a pure
/// function of (seed, file name).
#[test]
fn transcode_disk_classifies_refuses_unsupported_and_is_deterministic() {
    let sc = Scenario::new(3);
    let files: Vec<(&str, Vec<u8>)> = vec![
        ("lsm/MANIFEST", fixtures("lsm-manifest")[&1].clone()),
        ("lsm/wal-000000", fixtures("lsm-wal")[&1].clone()),
        ("lsm/sst-000001", fixtures("lsm-sstable")[&1].clone()),
        ("raft.wal", fixtures("control-wal")[&1].clone()),
        ("raftkv.wal.shared", fixtures("shared-wal")[&1].clone()),
        ("notes", b"not a format".to_vec()),
    ];
    for (f, b) in &files {
        block_on(sc.env().replace(f, b)).unwrap();
    }
    let before = sc.snapshot_disk();

    // One version back is unsupported today: an error, nothing rewritten.
    let err = transcode::transcode_disk(&sc.env(), 1, &TranscodeOpts::default()).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "{err}");
    assert_eq!(
        before,
        sc.snapshot_disk(),
        "a refused request must not touch the disk"
    );
    assert_eq!(transcode::supported_back(), &[0]);

    let r = transcode::transcode_disk(&sc.env(), 0, &TranscodeOpts::default()).unwrap();
    let names: Vec<&str> = r.transcoded.iter().map(|t| t.0.as_str()).collect();
    assert_eq!(
        names,
        [
            "lsm/MANIFEST",
            "lsm/sst-000001",
            "lsm/wal-000000",
            "raft.wal",
            "raftkv.wal.shared"
        ],
        "sorted name order"
    );
    assert_eq!(r.unrecognised, ["notes"]);
    assert!(r.kept.is_empty() && r.not_reached.is_empty());

    // Mixed-version: everything kept at 1000 permille; nothing kept at 0.
    let all_kept = TranscodeOpts {
        keep_current_fraction_permille: 1000,
        ..TranscodeOpts::default()
    };
    let r = transcode::transcode_disk(&sc.env(), 0, &all_kept).unwrap();
    assert!(r.transcoded.is_empty() && r.kept.len() == 5);

    // stop_after_files models a crash inside the transcode window.
    let stop = TranscodeOpts {
        stop_after_files: Some(2),
        ..TranscodeOpts::default()
    };
    let r = transcode::transcode_disk(&sc.env(), 0, &stop).unwrap();
    assert_eq!((r.transcoded.len(), r.not_reached.len()), (2, 3));

    // The keep decision depends only on (seed, file), never on call order.
    let half = |seed| TranscodeOpts {
        keep_current_fraction_permille: 500,
        seed,
        ..TranscodeOpts::default()
    };
    let pick = |seed| {
        transcode::transcode_disk(&sc.env(), 0, &half(seed))
            .unwrap()
            .kept
    };
    assert_eq!(pick(11), pick(11));
    assert!(
        (0..32u64).any(|s| pick(s) != pick(s + 1000)),
        "the seed must influence which files are kept"
    );
}
