//! A key's versions can straddle SSTable data blocks, and a historical point
//! read (`get_at`) must still find them (issue #1315).
//!
//! An SSTable flushes a data block as soon as it holds `TARGET_BLOCK_BYTES`
//! (4 KiB) of records, with no regard for key boundaries, and a memtable flush
//! writes **every** retained version of every key. A hot key whose rows are a
//! few KiB therefore lands its versions in several consecutive blocks. The
//! point-read path used to read exactly one block (the last whose first key is
//! at or below the key), so it saw only the newest few versions: `get_at(key,
//! v)` for an older `v` answered `None`, or, with an older table below it, a
//! *stale* row from that table. `animus-cp-data`'s abort lookback
//! (`get_at(key, intent_version - 1)`, for an intent that carries no prior) is
//! such a read, and a stale answer there rewrote one replica's row to an old
//! value for good (the lost acked appends of `upgrade-previous-release`).
//!
//! The property here is the engine's contract: `LsmEngine` is observationally
//! identical to `MemoryEngine` for every historical read, with no garbage
//! collection involved. Each cell drives one seeded op stream (big, variably
//! sized rows over a few hot keys and their lexical neighbours, a delete now
//! and then, manual flushes at random points so tables hold many versions per
//! key, and whole-engine restarts) through both engines, then compares
//! `get`/`get_at`/`scan_at`/`entries_at` at every version.
//!
//! Reproduce one seed with `ANIMUS_SEED=<seed>`; depth knob
//! `ANIMUS_LSM_MULTIBLOCK_SEEDS` (default 8).

use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, MemoryEngine, StorageEngine, Version};
use animus_test::corpus;
use futures::executor::block_on;

const PREFIX: &str = "db/";

fn opts() -> LsmOptions {
    LsmOptions {
        // Manual flushes only: tables are cut where the scenario says.
        flush_threshold_bytes: 1 << 30,
        // No compaction, so no garbage collection: every version stays on disk.
        compaction_trigger: 1 << 20,
        target_table_bytes: 1 << 20,
        level_fanout: 4,
        wal_segment_bytes: 1 << 20,
        tombstone_grace_versions: 1 << 40,
        trust_monotonic_versions: false,
        background_maintenance: false,
        ..LsmOptions::default()
    }
}

fn open(sim: &Simulator) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(sim.env(nid(0)), PREFIX, opts())).expect("open")
}

/// SplitMix64, local so the scenario is a pure function of its seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// The row for `version`: a few KiB (so two or three share a 4 KiB block at
/// most), content a pure function of `(version, len)` so a wrong version
/// returns visibly wrong bytes.
fn row(version: Version, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (version as usize).wrapping_mul(31).wrapping_add(i) as u8)
        .collect()
}

/// A short rendering of a read for assertion messages (rows are KiB long).
fn show(r: &Option<animus_storage::VersionedValue>) -> String {
    r.as_ref().map_or("absent".to_string(), |x| {
        format!(
            "v{} len {} first {:?}",
            x.version,
            x.value.len(),
            x.value.first()
        )
    })
}

fn show_all(r: &[(Vec<u8>, animus_storage::VersionedValue)]) -> String {
    r.iter()
        .map(|(k, x)| format!("{}={}", String::from_utf8_lossy(k), show(&Some(x.clone()))))
        .collect::<Vec<_>>()
        .join(", ")
}

fn scenario(seed: u64) {
    let sim = Simulator::new(seed);
    let mut lsm = open(&sim);
    let model = MemoryEngine::new();
    let mut rng = Rng(seed);

    // Hot keys, plus lexical neighbours (a block boundary can fall between a
    // hot key's versions and a neighbour's).
    let keys: [&[u8]; 5] = [b"j-neighbour", b"k0", b"k1", b"k2", b"l-neighbour"];
    let mut version: Version = 0;
    let ops = 60 + rng.below(80);
    for _ in 0..ops {
        let key = keys[rng.below(5) as usize];
        version += 1 + rng.below(3);
        if rng.below(8) == 0 {
            block_on(async {
                lsm.delete(key, version).await.expect("lsm delete");
                model.delete(key, version).await.expect("model delete");
            });
        } else {
            let len = 400 + rng.below(3400) as usize;
            let value = row(version, len);
            block_on(async {
                lsm.put(key, &value, version).await.expect("lsm put");
                model.put(key, &value, version).await.expect("model put");
            });
        }
        // Mid-stream (memtable plus tables, nothing flushed yet): a read at a
        // random version and the newest-version probe agree with the model.
        let probe_key = keys[rng.below(5) as usize];
        let probe_v = rng.below(version + 2);
        block_on(async {
            let got = lsm.get_at(probe_key, probe_v).await.expect("lsm get_at");
            let want = model
                .get_at(probe_key, probe_v)
                .await
                .expect("model get_at");
            assert!(
                got == want,
                "seed={seed}: mid-stream get_at({}, {probe_v}): engine {} vs model {}",
                String::from_utf8_lossy(probe_key),
                show(&got),
                show(&want)
            );
            let got = lsm.get(probe_key).await.expect("lsm get");
            let want = model.get(probe_key).await.expect("model get");
            assert!(got == want, "seed={seed}: mid-stream get mismatch");
            let got = StorageEngine::latest_version_incl_tombstone(&lsm, probe_key)
                .await
                .expect("lsm probe");
            let want = StorageEngine::latest_version_incl_tombstone(&model, probe_key)
                .await
                .expect("model probe");
            assert!(
                got == want,
                "seed={seed}: mid-stream probe {got:?} vs {want:?}"
            );
        });
        match rng.below(12) {
            0 | 1 => block_on(lsm.flush_now()).expect("flush"),
            2 => {
                // Whole-engine restart: everything unflushed comes back from
                // the WAL, everything flushed from its tables.
                drop(lsm);
                lsm = open(&sim);
            }
            _ => {}
        }
    }
    block_on(lsm.flush_now()).expect("final flush");

    block_on(async {
        for &key in &keys {
            for v in 0..=version + 1 {
                let got = lsm.get_at(key, v).await.expect("lsm get_at");
                let want = model.get_at(key, v).await.expect("model get_at");
                assert!(
                    got == want,
                    "seed={seed}: get_at({}, {v}): engine {} vs model {}",
                    String::from_utf8_lossy(key),
                    show(&got),
                    show(&want)
                );
                let mut end = key.to_vec();
                end.push(0);
                let got = lsm.scan_at(key, &end, v).await.expect("lsm scan_at");
                let want = model.scan_at(key, &end, v).await.expect("model scan_at");
                assert!(
                    got == want,
                    "seed={seed}: scan_at([{0}, {0}\\0), {v}): engine [{1}] vs model [{2}]",
                    String::from_utf8_lossy(key),
                    show_all(&got),
                    show_all(&want)
                );
            }
        }
        // The Bloom-gated newest-version probe (tombstones included) agrees
        // with the model's range-scan default (issue #1349), also for a key
        // never written.
        for key in keys.iter().copied().chain([b"zz-absent".as_slice()]) {
            let got = StorageEngine::latest_version_incl_tombstone(&lsm, key)
                .await
                .expect("lsm probe");
            let want = StorageEngine::latest_version_incl_tombstone(&model, key)
                .await
                .expect("model probe");
            assert!(
                got == want,
                "seed={seed}: latest_version_incl_tombstone({}): engine {got:?} vs model {want:?}",
                String::from_utf8_lossy(key)
            );
        }
        let (got, want) = (
            lsm.get(b"k0").await.expect("get"),
            model.get(b"k0").await.expect("get"),
        );
        assert!(
            got == want,
            "seed={seed}: latest k0: engine {} vs model {}",
            show(&got),
            show(&want)
        );
        for v in (0..=version + 1).step_by(7) {
            let got = lsm.entries_at(v).await.expect("lsm entries_at");
            let want = model.entries_at(v).await.expect("model entries_at");
            assert!(
                got == want,
                "seed={seed}: entries_at({v}): engine [{}] vs model [{}]",
                show_all(&got),
                show_all(&want)
            );
        }
    });
}

#[test]
fn historical_reads_find_versions_that_straddle_sstable_blocks() {
    if let Some(seed) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        scenario(seed);
        return;
    }
    let depth = corpus::seeds_from_env("ANIMUS_LSM_MULTIBLOCK_SEEDS").max(8);
    corpus::for_each_seed("lsm_multi_block_versions", depth, scenario);
}

/// The minimal shape, pinned (no randomness): one key, six 3000-byte
/// versions, one flush. Four of the six used to read back as absent.
#[test]
fn six_big_versions_of_one_key_in_one_table_all_read_back() {
    let sim = Simulator::new(1);
    let e = open(&sim);
    block_on(async {
        for v in 1u64..=6 {
            e.put(b"hot", &row(v, 3000), v).await.expect("put");
        }
        e.flush_now().await.expect("flush");
        for v in 1u64..=6 {
            let got = e.get_at(b"hot", v).await.expect("get_at");
            assert!(
                got.as_ref().map(|x| (x.version, x.value.clone())) == Some((v, row(v, 3000))),
                "get_at(hot, {v}): {}",
                show(&got)
            );
        }
    });
}
