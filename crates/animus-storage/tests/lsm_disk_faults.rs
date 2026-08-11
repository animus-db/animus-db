//! `LsmEngine` under the simulator's **disk fault model** (`DiskConfig`,
//! opt-in and seed-driven): injected I/O errors, torn WAL tails on crash, and
//! at-rest byte corruption. These are the rare-failure classes the old sim
//! disk could not produce (its crash dropped the whole un-synced buffer
//! atomically and never returned an error), so the engine's error-handling,
//! torn-record recovery, and checksum paths were unreachable under the
//! deterministic suite until now.
//!
//! Every scenario is a pure function of its seed (`ANIMUS_SEED` replays; the
//! seed is in every assertion message).
//!
//! Two tests pin **regressions for real gaps this fault model found and the
//! engine now fixes** (see each test's comment for the original diagnosis and
//! the fix):
//!
//! 1. [`acked_writes_after_torn_tail_recovery_survive_second_restart`] — a torn
//!    (garbage) WAL tail was correctly *skipped* by recovery, but was left in
//!    place in the reopened active segment, and post-recovery appends
//!    concatenated onto it with no separating frame boundary; the next
//!    recovery then dropped the first **acked** post-recovery record along
//!    with the garbage. Fixed by truncating a recovered active segment's torn
//!    tail (`Disk::replace`) before further appends ride it.
//! 2. [`corrupted_durable_wal_record_surfaces_loudly`] — WAL records carried no
//!    per-record checksum and `decode_wal` silently skipped *any* malformed
//!    line (not just a trailing torn one), so at-rest corruption of an acked,
//!    fsynced record was **silent data loss** instead of a loud error. Fixed by
//!    a length-prefixed + CRC32 binary frame per record (replacing the old
//!    newline-JSON encoding), with a hard error for any parse failure that
//!    isn't provably a trailing tear (a valid record still parses later in the
//!    file — see `decode_wal`'s doc comment in `lsm.rs`).

use animus_env::{Disk, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use futures::executor::block_on;

const PREFIX: &str = "db/";

/// Options that keep everything in the WAL (no auto-flush), so WAL-focused
/// fault tests control exactly which files exist.
fn wal_only_opts() -> LsmOptions {
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

/// Options with small thresholds so flushes and compactions happen mid-test.
fn churn_opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 256,
        compaction_trigger: 3,
        target_table_bytes: 1024,
        level_fanout: 2,
        wal_segment_bytes: 192,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: false,
        background_maintenance: false,
    }
}

fn open(sim: &Simulator, opts: LsmOptions) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(sim.env(nid(0)), PREFIX, opts)).expect("open")
}

fn key(i: u64) -> String {
    format!("k{i:03}")
}

fn value(i: u64) -> String {
    format!("v{i}")
}

/// The active (highest-numbered) WAL segment's file name.
fn active_wal_file(e: &LsmEngine<SimEnv>) -> String {
    let seg = *e.wal_segments().last().expect("an active segment exists");
    format!("{PREFIX}wal-{seg:06}")
}

/// The byte length of the first complete WAL frame in `bytes`. The on-disk
/// framing is `len(u32 BE) | crc32(u32 BE) | payload` (see `lsm.rs`'s
/// `encode_wal`); this reads just the length header to find the frame boundary.
fn first_frame_len(bytes: &[u8]) -> usize {
    let len = u32::from_be_bytes(bytes[0..4].try_into().expect("frame header present")) as usize;
    8 + len
}

/// Simulate a crash **mid group-commit append**: buffer (without syncing) one
/// more record's bytes onto the active WAL segment — exactly what the disk
/// holds when the power cuts between `append` and `sync` — then crash with a
/// torn-tail model, so a seed-chosen strict prefix of that record survives.
/// The buffered bytes are a copy of the segment's first (durable, complete)
/// frame, so if the tear happens to retain the whole frame the replay is an
/// idempotent duplicate — the interesting cases are the partial ones.
async fn buffer_unsynced_wal_record(sim: &Simulator, e: &LsmEngine<SimEnv>) {
    let env = sim.env(nid(0));
    let file = active_wal_file(e);
    let bytes = env.read(&file).await.expect("read wal segment");
    let frame_len = first_frame_len(&bytes);
    env.append(&file, &bytes[..frame_len])
        .await
        .expect("append un-synced record");
}

/// Torn-tail crash during a WAL write: recovery must drop the torn trailing
/// record, keep **every** acked (returned) write, and leave the engine
/// usable. Swept across seeds so the tear point varies over the whole record;
/// a `corrupt_on_crash` variant garbles the retained bytes too.
#[test]
fn torn_wal_tail_crash_recovers_all_acked_writes() {
    for corrupt in [false, true] {
        for seed in [0xA1u64, 0xB2, 0xC3, 7, 42, 1337] {
            let sim = Simulator::new(seed);
            let mut cfg = DiskConfig::default();
            cfg.torn_tail_on_crash = true;
            cfg.corrupt_on_crash = corrupt;
            sim.set_disk_config(cfg);

            let n = 10u64;
            {
                let e = open(&sim, wal_only_opts());
                block_on(async {
                    for i in 0..n {
                        e.put(key(i).as_bytes(), value(i).as_bytes(), i + 1)
                            .await
                            .unwrap();
                    }
                    buffer_unsynced_wal_record(&sim, &e).await;
                });
            }
            sim.crash(nid(0));

            let e = open(&sim, wal_only_opts());
            block_on(async {
                for i in 0..n {
                    assert_eq!(
                        e.get(key(i).as_bytes()).await.unwrap().unwrap().value,
                        value(i).as_bytes(),
                        "seed={seed} corrupt={corrupt}: acked key {} lost to a torn tail",
                        key(i),
                    );
                }
                // The engine stays fully usable after recovering a torn tail.
                e.put(b"post", b"recovery", n + 1).await.unwrap();
                assert_eq!(
                    e.get(b"post").await.unwrap().unwrap().value,
                    b"recovery",
                    "seed={seed} corrupt={corrupt}",
                );
            });
        }
    }
}

/// Injected WAL `append`/`sync` errors on the write path: a failing write
/// returns `Err` (no silent ack), the engine stays usable once the fault
/// clears, and after a crash + reopen **every write that returned `Ok` is
/// present**. (A write that returned `Err` is indeterminate — like a real
/// failed fsync it may or may not survive — so nothing is asserted about it.)
#[test]
fn injected_wal_errors_surface_and_lose_no_acked_write() {
    for seed in [0xE1u64, 0xE2, 0xE3, 0xE4, 5, 99] {
        let sim = Simulator::new(seed);
        let n = 40u64;
        let mut acked = Vec::new();
        let mut errors = 0u32;
        {
            let e = open(&sim, wal_only_opts());
            // Enable a 30% per-op disk error rate only once the engine is open.
            let mut cfg = DiskConfig::default();
            cfg.set_error_prob(0.3);
            sim.set_disk_config(cfg);
            block_on(async {
                for i in 0..n {
                    match e.put(key(i).as_bytes(), value(i).as_bytes(), i + 1).await {
                        Ok(()) => acked.push(i),
                        Err(_) => errors += 1,
                    }
                }
            });
            // Clear the fault: the engine must accept writes again.
            sim.set_disk_config(DiskConfig::default());
            block_on(async {
                e.put(b"after-clear", b"ok", n + 1)
                    .await
                    .unwrap_or_else(|err| {
                        panic!("seed={seed}: engine unusable after faults cleared: {err}")
                    });
            });
            acked.push(u64::MAX); // sentinel for the post-clear write, checked below
        }
        assert!(
            errors > 0,
            "seed={seed}: p=0.3 over {n} puts should inject at least one error"
        );
        assert!(
            acked.len() > 1,
            "seed={seed}: some puts should have succeeded"
        );

        sim.crash(nid(0));
        let e = open(&sim, wal_only_opts());
        block_on(async {
            for &i in &acked {
                let (k, v) = if i == u64::MAX {
                    ("after-clear".to_string(), "ok".to_string())
                } else {
                    (key(i), value(i))
                };
                let got = e.get(k.as_bytes()).await.unwrap();
                assert_eq!(
                    got.as_ref().map(|vv| vv.value.clone()),
                    Some(v.clone().into_bytes()),
                    "seed={seed}: acked write {k} lost after injected-error run + crash",
                );
            }
        });
    }
}

/// Injected errors while flushes and compactions are firing: every op either
/// succeeds or fails loudly (no panic, no hang), and after clearing the fault
/// and crashing, every acked write is still present. This drives the flush /
/// manifest-swap / compaction error paths that were unreachable before.
#[test]
fn injected_errors_during_flush_and_compaction_lose_no_acked_write() {
    for seed in [0xF1u64, 0xF2, 0xF3, 21] {
        let sim = Simulator::new(seed);
        let n = 120u64;
        let mut acked = Vec::new();
        {
            let e = open(&sim, churn_opts());
            let mut cfg = DiskConfig::default();
            cfg.set_error_prob(0.05);
            sim.set_disk_config(cfg);
            block_on(async {
                for i in 0..n {
                    if e.put(key(i).as_bytes(), value(i).as_bytes(), i + 1)
                        .await
                        .is_ok()
                    {
                        acked.push(i);
                    }
                }
            });
            sim.set_disk_config(DiskConfig::default());
        }
        assert!(
            !acked.is_empty(),
            "seed={seed}: some puts should have succeeded"
        );

        sim.crash(nid(0));
        let e = open(&sim, churn_opts());
        block_on(async {
            for &i in &acked {
                let got = e.get(key(i).as_bytes()).await.unwrap();
                assert_eq!(
                    got.as_ref().map(|vv| vv.value.clone()),
                    Some(value(i).into_bytes()),
                    "seed={seed}: acked write {} lost across faulty flush/compaction run",
                    key(i),
                );
            }
        });
    }
}

/// At-rest corruption of a **synced SSTable data block** surfaces as a clean
/// `StorageError` on read — the per-block CRC catches it — never a panic or
/// silently wrong data.
#[test]
fn corrupted_sstable_block_read_is_a_clean_error() {
    let seed = 0x557u64;
    let sim = Simulator::new(seed);
    let e = open(&sim, wal_only_opts());
    block_on(async {
        for i in 0..30u64 {
            e.put(key(i).as_bytes(), value(i).as_bytes(), i + 1)
                .await
                .unwrap();
        }
        // One explicit flush: everything moves to a single SSTable and the
        // memtable is cleared, so subsequent point reads must hit disk.
        e.flush_now().await.unwrap();
        assert_eq!(e.sstable_count(), 1, "seed={seed}: expected one SSTable");
    });
    assert_eq!(
        e.memtable_len(),
        0,
        "seed={seed}: memtable must be empty so the read goes to the SSTable"
    );
    let views = e.sstable_views();
    let sst_file = format!("{PREFIX}sst-{:06}", views[0].seq);
    // Sanity: uncorrupted, the on-disk read works.
    block_on(async {
        assert_eq!(
            e.get(key(0).as_bytes()).await.unwrap().unwrap().value,
            value(0).as_bytes(),
            "seed={seed}"
        );
    });
    // Flip one byte inside the first data block (blocks start at offset 0; the
    // per-block CRC covers `tag || payload`, so any flipped payload byte must
    // be detected).
    assert!(
        sim.corrupt_durable(nid(0), &sst_file, 1),
        "seed={seed}: corruption must land"
    );
    block_on(async {
        let err = e
            .get(key(0).as_bytes())
            .await
            .expect_err("seed={seed}: a corrupted block must fail the read, not return data");
        let msg = err.to_string();
        assert!(
            msg.contains("crc") || msg.contains("corrupt") || msg.contains("decompress"),
            "seed={seed}: expected a checksum/corruption error, got: {msg}"
        );
    });
}

/// At-rest corruption of the durable MANIFEST fails `open` with a clean
/// `StorageError` — never a panic (the length-prefixed binary codec must
/// bounds-check every field against flipped bytes).
#[test]
fn corrupted_manifest_fails_open_cleanly() {
    for offset in [4u64, 6, 13, 22, 40] {
        let seed = 0x3AF0u64 + offset;
        let sim = Simulator::new(seed);
        {
            let e = open(&sim, wal_only_opts());
            block_on(async {
                for i in 0..30u64 {
                    e.put(key(i).as_bytes(), value(i).as_bytes(), i + 1)
                        .await
                        .unwrap();
                }
                e.flush_now().await.unwrap(); // writes a real manifest
            });
        }
        sim.crash(nid(0));
        if !sim.corrupt_durable(nid(0), "db/MANIFEST", offset) {
            continue; // manifest shorter than this offset; nothing to test
        }
        // Must be a clean error (or, for a byte the codec ignores, a clean
        // open) — never a panic.
        match block_on(LsmEngine::open_with(
            sim.env(nid(0)),
            PREFIX,
            wal_only_opts(),
        )) {
            Ok(_) | Err(_) => {}
        }
    }
}

/// REGRESSION for a real bug this fault model found: after a torn-tail crash,
/// recovery correctly skipped the torn trailing bytes but left them in the
/// reopened **active** WAL segment. The next write's record was appended
/// directly after the garbage with no frame boundary in between, so on a
/// *second* recovery that acked record was glued to the garbage and
/// `decode_wal` silently dropped it — losing an acked, fsynced write.
///
/// Diagnosis: `LsmEngine::open_with_metrics` replayed the live segments
/// through `decode_wal` (which tolerates a torn trailing record) but never
/// truncated or sealed the torn tail; `GroupCommit::new` reopens the highest
/// live segment as the active one and appends ride the raw end of file. Fixed
/// by truncating the segment to its last well-formed frame boundary on open
/// (via `Disk::replace`) whenever a torn tail was detected.
///
/// Deterministic repro: seed 0xA1 (any seed whose tear keeps > 0 bytes, which
/// is nearly all of them — the tear point is uniform over the whole record).
#[test]
fn acked_writes_after_torn_tail_recovery_survive_second_restart() {
    // Sweep seeds: the bug fires whenever the tear retains at least one byte.
    for seed in [0xA1u64, 0xB2, 0xC3, 7, 42, 1337] {
        let sim = Simulator::new(seed);
        let mut cfg = DiskConfig::default();
        cfg.torn_tail_on_crash = true;
        sim.set_disk_config(cfg);

        let n = 10u64;
        {
            let e = open(&sim, wal_only_opts());
            block_on(async {
                for i in 0..n {
                    e.put(key(i).as_bytes(), value(i).as_bytes(), i + 1)
                        .await
                        .unwrap();
                }
                buffer_unsynced_wal_record(&sim, &e).await;
            });
        }
        sim.crash(nid(0)); // tear: a partial record is left at the segment's end

        // First recovery: correct (the torn line is skipped). Now write one
        // more record — it is acked (WAL-synced before returning).
        {
            let e = open(&sim, wal_only_opts());
            block_on(async {
                for i in 0..n {
                    assert!(
                        e.get(key(i).as_bytes()).await.unwrap().is_some(),
                        "seed={seed}: first recovery lost {}",
                        key(i)
                    );
                }
                e.put(b"post-tear", b"acked", n + 1).await.unwrap();
                assert_eq!(
                    e.get(b"post-tear").await.unwrap().unwrap().value,
                    b"acked",
                    "seed={seed}"
                );
            });
        }
        // Clean restart: nothing is buffered (the put synced before returning),
        // so this crash tears nothing — it only forces a second replay.
        sim.crash(nid(0));

        let e = open(&sim, wal_only_opts());
        block_on(async {
            assert_eq!(
                e.get(b"post-tear").await.unwrap().map(|v| v.value),
                Some(b"acked".to_vec()),
                "seed={seed}: acked post-recovery write lost on the second \
                 restart — its WAL record was concatenated onto the torn \
                 garbage left in the active segment",
            );
        });
    }
}

/// REGRESSION for a real gap this fault model found: the WAL used to carry
/// **no per-record checksum**, and `decode_wal` silently skipped *any* line
/// that failed to parse — not just a trailing torn one. So at-rest corruption
/// of a durable, acked, fsynced WAL record was silent data loss: the engine
/// opened cleanly and simply forgot the write. (Contrast: an SSTable block has
/// a CRC and the same corruption fails the read loudly —
/// `corrupted_sstable_block_read_is_a_clean_error`.)
///
/// Fixed by framing WAL records with a length + CRC32 (as SSTable blocks are),
/// with a hard error for a malformed record that is *not* provably a trailing
/// tear (a valid record still parses later in the file — see `decode_wal`'s
/// doc comment in `lsm.rs`).
///
/// Deterministic repro (no RNG involved): any seed.
#[test]
fn corrupted_durable_wal_record_surfaces_loudly() {
    let seed = 0xBADu64;
    let sim = Simulator::new(seed);
    {
        let e = open(&sim, wal_only_opts());
        block_on(async {
            for i in 0..5u64 {
                e.put(key(i).as_bytes(), value(i).as_bytes(), i + 1)
                    .await
                    .unwrap();
            }
        });
    }
    sim.crash(nid(0));
    // Flip one byte inside the first (durable, acked) record of the WAL.
    assert!(
        sim.corrupt_durable(nid(0), &format!("{PREFIX}wal-000000"), 1),
        "seed={seed}: corruption must land"
    );

    // The engine must not silently drop an acked write: either `open` fails
    // loudly, or the write is still readable. Today neither holds — open
    // succeeds and k000 is gone.
    match block_on(LsmEngine::open_with(
        sim.env(nid(0)),
        PREFIX,
        wal_only_opts(),
    )) {
        Err(_) => {} // loud failure: acceptable
        Ok(e) => block_on(async {
            assert_eq!(
                e.get(key(0).as_bytes()).await.unwrap().map(|v| v.value),
                Some(value(0).into_bytes()),
                "seed={seed}: acked write k000 silently lost to WAL corruption \
                 (no per-record checksum; decode_wal skips malformed lines)",
            );
        }),
    }
}
