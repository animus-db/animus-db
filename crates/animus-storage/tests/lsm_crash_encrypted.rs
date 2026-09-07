//! Crash-safety of `LsmEngine` when its `Disk` seam is wrapped in
//! `EncryptedDisk`/`EncryptedEnv` (ADR 0069, S-03 PR 1) — the encrypted
//! sibling of `lsm_crash.rs`'s plain-`SimEnv` corpus, over the same
//! deterministic disk model but composed with AEAD framing.
//!
//! The property this file exists to pin, beyond what `lsm_crash.rs` already
//! proves for the plaintext path: **a torn or corrupted write must never
//! partially decrypt** — `DiskConfig::torn_tail_on_crash`/`corrupt_on_crash`
//! can cut a frame's length prefix, ciphertext, or authentication tag at any
//! point, and recovery must always land on the last complete, authenticated
//! frame (silently discarding the torn one), never surface garbage
//! plaintext or a spurious "wrong key" refusal for what is actually an
//! ordinary crash.
//!
//! **Corpus shape**: same `animus_test::corpus` scaffolding as
//! `lsm_crash.rs`/`lsm_disk_faults.rs`; depth knob
//! **`ANIMUS_LSM_ENCRYPTED_SEEDS`** (default 1 = the frozen cells below).

use animus_env::{Disk, EncryptedEnv, EncryptionKey, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use animus_test::corpus::{self, SeedVariant};
use futures::executor::block_on;
use std::collections::BTreeSet;

const PREFIX: &str = "db/";

fn test_key() -> EncryptionKey {
    EncryptionKey::from_bytes([0x42; 32])
}

fn other_key() -> EncryptionKey {
    EncryptionKey::from_bytes([0x99; 32])
}

fn opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 128,
        compaction_trigger: 3,
        target_table_bytes: 512,
        level_fanout: 2,
        wal_segment_bytes: 96,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: false,
        background_maintenance: false,
    }
}

async fn open_env(sim: &Simulator, key: EncryptionKey) -> std::io::Result<EncryptedEnv<SimEnv>> {
    EncryptedEnv::open(sim.env(nid(0)), key).await
}

fn open(sim: &Simulator) -> LsmEngine<EncryptedEnv<SimEnv>> {
    let env = block_on(open_env(sim, test_key())).expect("open encrypted env");
    block_on(LsmEngine::open_with(env, PREFIX, opts())).expect("open engine")
}

/// No auto-flush, one large WAL segment — so a handful of `put`s land, in
/// order, as consecutive frames inside a single `db/wal-000000` file we can
/// target with `Simulator::corrupt_durable`.
fn no_flush_opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 1 << 20,
        compaction_trigger: 1 << 20,
        target_table_bytes: 1 << 20,
        level_fanout: 4,
        wal_segment_bytes: 1 << 20,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: false,
        background_maintenance: false,
    }
}

/// 1. The plaintext corpus's core property (writes that returned survive a
///    clean crash + reopen), replayed through the encrypted wrapper.
fn scenario_synced_writes_survive_crash(seed: u64) {
    let sim = Simulator::new(seed);
    {
        let e = open(&sim);
        block_on(async {
            e.put(b"alpha", b"1", 1).await.unwrap();
            e.put(b"beta", b"2", 2).await.unwrap();
            e.delete(b"alpha", 3).await.unwrap();
            e.put(b"gamma", b"3", 4).await.unwrap();
        });
    }
    sim.crash(nid(0));

    let e = open(&sim);
    block_on(async {
        assert_eq!(e.get(b"alpha").await.unwrap(), None, "seed={seed}: deleted");
        assert_eq!(
            e.get(b"beta").await.unwrap().unwrap().value,
            b"2",
            "seed={seed}"
        );
        assert_eq!(
            e.get(b"gamma").await.unwrap().unwrap().value,
            b"3",
            "seed={seed}"
        );
    });
}

/// 2. A flush forces a real SSTable (not just the WAL) through the encrypted
///    `replace`/`append` paths; reopen must decrypt it back intact.
fn scenario_flushed_sstable_survives_crash(seed: u64) {
    let sim = Simulator::new(seed);
    let count = 50u64;
    {
        let e = open(&sim);
        block_on(async {
            for i in 0..count {
                let k = format!("k{i:03}");
                e.put(k.as_bytes(), format!("v{i}").as_bytes(), i + 1)
                    .await
                    .unwrap();
            }
            assert!(
                e.sstable_count() >= 1,
                "seed={seed}: expected a flush, got {} sstables",
                e.sstable_count()
            );
        });
    }
    sim.crash(nid(0));

    let e = open(&sim);
    block_on(async {
        for i in 0..count {
            let k = format!("k{i:03}");
            assert_eq!(
                e.get(k.as_bytes()).await.unwrap().unwrap().value,
                format!("v{i}").as_bytes(),
                "seed={seed}: key {k}"
            );
        }
    });
}

/// 3. `torn_tail_on_crash`: an in-flight, unsynced append is cut at a
///    seed-chosen byte offset — inside the length prefix, inside the
///    ciphertext, or inside the AEAD tag. Recovery must discard the torn
///    frame as a unit (never serve a partially-decrypted value) and every
///    previously **synced** write must still read back correctly.
fn scenario_torn_tail_never_partially_decrypts(seed: u64) {
    let sim = Simulator::new(seed);
    let mut cfg = DiskConfig::default();
    cfg.torn_tail_on_crash = true;
    sim.set_disk_config(cfg);
    {
        let e = open(&sim);
        block_on(async {
            for i in 0..30u64 {
                let k = format!("key{i:03}");
                e.put(k.as_bytes(), format!("val{i}").as_bytes(), i + 1)
                    .await
                    .unwrap();
            }
        });
    }
    sim.crash(nid(0));

    // Reopen must succeed (never a spurious auth-failure refusal for what is
    // an ordinary torn tail) and every write made it through `put`, which
    // only returns after its WAL append is synced — so all 30 keys are
    // durable and must read back exactly as written, byte for byte.
    let e = open(&sim);
    block_on(async {
        for i in 0..30u64 {
            let k = format!("key{i:03}");
            assert_eq!(
                e.get(k.as_bytes()).await.unwrap().unwrap().value,
                format!("val{i}").as_bytes(),
                "seed={seed}: key {k} corrupted or lost after a torn-tail crash"
            );
        }
    });
}

/// 4. Like scenario 3, but `corrupt_on_crash` additionally flips a byte
///    inside the retained torn region — a garbled, not just truncated, final
///    write. Still must recover cleanly: the corrupted bytes live only in
///    the discarded frame.
fn scenario_corrupt_tail_never_partially_decrypts(seed: u64) {
    let sim = Simulator::new(seed);
    let mut cfg = DiskConfig::default();
    cfg.torn_tail_on_crash = true;
    cfg.corrupt_on_crash = true;
    sim.set_disk_config(cfg);
    {
        let e = open(&sim);
        block_on(async {
            for i in 0..30u64 {
                let k = format!("key{i:03}");
                e.put(k.as_bytes(), format!("val{i}").as_bytes(), i + 1)
                    .await
                    .unwrap();
            }
        });
    }
    sim.crash(nid(0));

    let e = open(&sim);
    block_on(async {
        for i in 0..30u64 {
            let k = format!("key{i:03}");
            assert_eq!(
                e.get(k.as_bytes()).await.unwrap().unwrap().value,
                format!("val{i}").as_bytes(),
                "seed={seed}: key {k} corrupted or lost after a corrupted-tail crash"
            );
        }
    });
}

/// 5. A wrong key against an already-encrypted data directory is refused
///    loudly at `EncryptedEnv::open` — never a silent reset, never a partial
///    open — and the exact refusal text names the mismatch.
fn scenario_wrong_key_on_reopen_is_loud_refusal(seed: u64) {
    let sim = Simulator::new(seed);
    {
        let e = open(&sim);
        block_on(async {
            e.put(b"k", b"v", 1).await.unwrap();
        });
    }

    let msg = match block_on(open_env(&sim, other_key())) {
        Ok(_) => panic!("seed={seed}: wrong key must be refused, not accepted"),
        Err(e) => e.to_string(),
    };
    assert!(
        msg.contains("does not match the key this data directory was encrypted with"),
        "seed={seed}: unexpected refusal text: {msg}"
    );

    // The right key still opens it, and the earlier write is intact — the
    // refusal above touched nothing.
    let e = open(&sim);
    block_on(async {
        assert_eq!(
            e.get(b"k").await.unwrap().unwrap().value,
            b"v",
            "seed={seed}"
        );
    });
}

/// 6. No key at all against an already-encrypted data directory is refused
///    just as loudly.
fn scenario_missing_key_on_reopen_is_loud_refusal(seed: u64) {
    let sim = Simulator::new(seed);
    {
        let e = open(&sim);
        block_on(async {
            e.put(b"k", b"v", 1).await.unwrap();
        });
    }

    let plain_env = sim.env(nid(0));
    let err = block_on(animus_env::verify_or_init_marker(
        &plain_env, &plain_env, None,
    ))
    .expect_err("missing key against an encrypted directory must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains("no --encryption-key was given"),
        "seed={seed}: unexpected refusal text: {msg}"
    );
}

/// 7. At-rest corruption of an already-durable, non-trailing frame (bit rot,
///    or here `Simulator::corrupt_durable`, independent of any crash) must
///    surface as a hard error — never a silent truncation that drops
///    everything from the corrupted frame onward even though later, intact
///    frames were sitting right there on disk (see `Scan::Corrupted`'s
///    doc). No crash occurs in this scenario at all.
fn scenario_mid_file_corruption_of_durable_frame_is_a_hard_error(seed: u64) {
    let sim = Simulator::new(seed);
    {
        let env = block_on(open_env(&sim, test_key())).expect("open encrypted env");
        let e: LsmEngine<EncryptedEnv<SimEnv>> =
            block_on(LsmEngine::open_with(env, PREFIX, no_flush_opts())).expect("open engine");
        block_on(async {
            for i in 0..10u64 {
                let k = format!("key{i:03}");
                e.put(k.as_bytes(), format!("val{i}").as_bytes(), i + 1)
                    .await
                    .unwrap();
            }
        });
    }

    // Flip a byte a couple bytes into frame 0's ciphertext (past the
    // 21-byte encrypted-file header and frame 0's own 4-byte length
    // prefix) — corrupting the first WAL record's ciphertext while leaving
    // every later record (still on disk, unsynced-crash-free) intact.
    let wal_file = format!("{PREFIX}wal-000000");
    let corrupted = sim.corrupt_durable(nid(0), &wal_file, 27);
    assert!(
        corrupted,
        "seed={seed}: corruption must land inside an existing durable file"
    );

    match block_on(open_env(&sim, test_key())) {
        Err(e) => panic!("seed={seed}: marker check must not itself fail here: {e}"),
        Ok(env) => match block_on(LsmEngine::<EncryptedEnv<SimEnv>>::open_with(
            env,
            PREFIX,
            no_flush_opts(),
        )) {
            Err(_) => {} // loud failure: exactly the required behaviour
            Ok(_) => panic!(
                "seed={seed}: mid-file corruption of a durable frame with valid frames after \
                 it was silently accepted instead of refused"
            ),
        },
    }
}

// ---------------------------------------------------------------------------
// The frozen corpus.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Scenario {
    name: String,
    seed: u64,
    run: fn(u64),
}

impl SeedVariant for Scenario {
    fn scenario_name(&self) -> &str {
        &self.name
    }
    fn reseeded(&self, name: String, seed: u64) -> Self {
        Scenario {
            name,
            seed,
            run: self.run,
        }
    }
}

fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_LSM_ENCRYPTED_SEEDS")
}

macro_rules! scenario {
    ($name:expr, $f:ident) => {
        Scenario {
            name: $name.to_string(),
            seed: corpus::name_seed($name),
            run: $f,
        }
    };
}

fn scenario_cells() -> Vec<Scenario> {
    vec![
        scenario!(
            "encrypted_synced_writes_survive_crash",
            scenario_synced_writes_survive_crash
        ),
        scenario!(
            "encrypted_flushed_sstable_survives_crash",
            scenario_flushed_sstable_survives_crash
        ),
        scenario!(
            "encrypted_torn_tail_never_partially_decrypts",
            scenario_torn_tail_never_partially_decrypts
        ),
        scenario!(
            "encrypted_corrupt_tail_never_partially_decrypts",
            scenario_corrupt_tail_never_partially_decrypts
        ),
        scenario!(
            "encrypted_wrong_key_on_reopen_is_loud_refusal",
            scenario_wrong_key_on_reopen_is_loud_refusal
        ),
        scenario!(
            "encrypted_missing_key_on_reopen_is_loud_refusal",
            scenario_missing_key_on_reopen_is_loud_refusal
        ),
        scenario!(
            "encrypted_mid_file_corruption_of_durable_frame_is_a_hard_error",
            scenario_mid_file_corruption_of_durable_frame_is_a_hard_error
        ),
    ]
}

fn corpus() -> Vec<Scenario> {
    corpus::seed_expand(scenario_cells(), seeds_per_cell())
}

#[test]
fn lsm_crash_encrypted_corpus_runs_every_scenario() {
    for s in corpus() {
        (s.run)(s.seed);
    }
}

#[test]
fn lsm_crash_encrypted_corpus_names_and_seeds_are_unique_and_frozen() {
    let cells = scenario_cells();
    assert!(
        cells.len() >= 6,
        "corpus shrank unexpectedly to {} cells",
        cells.len()
    );
    let names: BTreeSet<&str> = cells.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names.len(), cells.len(), "corpus names must be unique");
    let seeds: BTreeSet<u64> = cells.iter().map(|s| s.seed).collect();
    assert_eq!(seeds.len(), cells.len(), "corpus seeds must be unique");
    for cell in &cells {
        assert_eq!(
            cell.seed,
            corpus::name_seed(&cell.name),
            "frozen seed moved for {}",
            cell.name
        );
    }
}

#[test]
fn lsm_crash_encrypted_scenario_is_reproducible_from_its_seed() {
    let seed = corpus::name_seed("encrypted_torn_tail_never_partially_decrypts");
    scenario_torn_tail_never_partially_decrypts(seed);
    scenario_torn_tail_never_partially_decrypts(seed);
}

/// Sanity check independent of the corpus machinery: the marker file itself
/// is invisible through `EncryptedEnv`'s own `Disk::list`, and no plaintext
/// value ever lands on the underlying (unwrapped) disk.
#[test]
fn lsm_crash_encrypted_marker_is_hidden_and_values_are_never_plaintext_on_disk() {
    let sim = Simulator::new(corpus::name_seed("marker_hidden"));
    let e = open(&sim);
    block_on(async {
        e.put(b"super-secret-key", b"super-secret-value", 1)
            .await
            .unwrap();
    });

    let raw_env = sim.env(nid(0));
    let names = block_on(raw_env.list()).unwrap();
    assert!(
        names.iter().any(|n| n == animus_env::MARKER_FILE),
        "the raw disk must actually hold the marker file"
    );
    let encrypted_names = block_on(open_env(&sim, test_key())).unwrap();
    let listed = block_on(Disk::list(&encrypted_names)).unwrap();
    assert!(
        !listed.iter().any(|n| n == animus_env::MARKER_FILE),
        "EncryptedEnv::list must hide the marker file, got {listed:?}"
    );

    for name in names {
        let raw = block_on(raw_env.read(&name)).unwrap();
        assert!(
            !contains_subslice(&raw, b"super-secret-value"),
            "plaintext value leaked into raw file {name}"
        );
        assert!(
            !contains_subslice(&raw, b"super-secret-key"),
            "plaintext key leaked into raw file {name}"
        );
    }
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}
