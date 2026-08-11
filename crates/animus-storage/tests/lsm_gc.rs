//! Tombstone garbage collection during compaction (ADR 0008/0010).
//!
//! A tombstone (and the versions it shadows) is reclaimed during compaction once
//! it has aged below the **GC floor** (`max_version - tombstone_grace_versions`)
//! and no deeper, uncompacted level could still hold an older value for the key.
//! Reclamation must be **invisible above the floor**: a `get`/`get_at` in the
//! retained window reads exactly as before, and the only on-disk effect is that
//! the shadowed versions and the reclaimed tombstone are physically gone.
//!
//! These run under the deterministic `SimEnv`, so every property is reproducible
//! from the seed in the assertion messages.

use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, Snapshot, StorageEngine};
use futures::executor::block_on;

const PREFIX: &str = "db/";

/// Small flush threshold + low compaction trigger so a handful of writes flush and
/// compact; a small grace so a tombstone ages below the floor quickly.
fn opts(grace: u64) -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 64,
        compaction_trigger: 2,
        target_table_bytes: 256,
        level_fanout: 2,
        wal_segment_bytes: 96,
        tombstone_grace_versions: grace,
        trust_monotonic_versions: false,
        background_maintenance: false,
    }
}

fn open(sim: &Simulator, grace: u64) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(sim.env(nid(0)), PREFIX, opts(grace))).expect("open")
}

/// A tombstone that has aged below the GC floor is reclaimed during compaction:
/// the key reads as absent, the on-disk table no longer holds the tombstone *or*
/// the value it shadowed. A second key deleted *within* the grace window keeps its
/// tombstone on disk (so anti-entropy can still propagate the delete).
#[test]
fn aged_tombstone_is_reclaimed_recent_one_is_preserved() {
    let seed = 0x6C_u64;
    let sim = Simulator::new(seed);
    // Grace wide enough that the recent delete (near max) stays inside the retained
    // window while the victim (deleted at version 2) ages well below the floor.
    let grace = 200;
    let e = open(&sim, grace);
    block_on(async {
        // The victim: written then deleted at low versions (1, 2). It will age out.
        e.put(b"victim", b"hello", 1).await.unwrap();
        e.delete(b"victim", 2).await.unwrap();

        // Churn many other keys to (a) push max_version far above the victim's
        // tombstone version and (b) force repeated flushes + compactions that fold
        // the victim's history together.
        for i in 0u64..200 {
            let k = format!("k{i:04}");
            e.put(k.as_bytes(), format!("v{i}").as_bytes(), 100 + i)
                .await
                .unwrap();
        }

        // A recent delete: its tombstone version sits just below max, inside the
        // grace window (floor = max - grace), so GC must preserve it.
        let recent_ver = e.latest_version() + 1;
        e.put(b"recent", b"data", recent_ver).await.unwrap();
        e.delete(b"recent", recent_ver + 1).await.unwrap();

        // Force the memtable holding the recent tombstone to flush + compact, using
        // versions only just above it so the recent tombstone stays within grace.
        for i in 0u64..60 {
            let k = format!("z{i:04}");
            e.put(k.as_bytes(), format!("z{i}").as_bytes(), recent_ver + 2 + i)
                .await
                .unwrap();
        }

        assert!(
            e.compaction_count() >= 1,
            "seed={seed}: expected a compaction to run GC"
        );

        // Observable: the victim reads absent (tombstone reclaimed => nothing left),
        // and a historical read at its original version is also absent (the shadowed
        // value is gone too — it was below the floor).
        assert_eq!(
            e.get(b"victim").await.unwrap(),
            None,
            "seed={seed}: reclaimed key must read absent"
        );
        assert_eq!(
            e.get_at(b"victim", 1).await.unwrap(),
            None,
            "seed={seed}: shadowed value must be gone, not resurrected"
        );

        // Physical: the disk holds NO records for the victim at all (tombstone +
        // shadowed value both reclaimed).
        let victim_disk = e.test_disk_versions_of(b"victim").await;
        assert!(
            victim_disk.is_empty(),
            "seed={seed}: victim still on disk after GC: {victim_disk:?}"
        );

        // The recent tombstone is within grace, so it is preserved (the key reads
        // absent, but the tombstone is retained on disk for anti-entropy).
        assert_eq!(
            e.get(b"recent").await.unwrap(),
            None,
            "seed={seed}: recent delete reads absent"
        );
        let recent_disk = e.test_disk_versions_of(b"recent").await;
        assert!(
            recent_disk
                .iter()
                .any(|&(v, is_ts)| is_ts && v == recent_ver + 1),
            "seed={seed}: within-grace tombstone must be preserved on disk: {recent_disk:?}"
        );
    });
}

/// GC never resurrects a key whose value lives in a deeper, uncompacted level: if
/// a compaction at level L holds the tombstone but a level > L table still holds an
/// older value for that key, the tombstone is **retained** (only the versions
/// strictly below it are reclaimed), so the key keeps reading absent.
#[test]
fn gc_does_not_resurrect_across_a_deeper_level() {
    let seed = 0xDEE9_u64;
    let sim = Simulator::new(seed);
    // Tiny grace so everything is below the floor quickly; the safety here comes
    // from the deeper-level guard, not the grace window.
    let e = open(&sim, 1);
    block_on(async {
        // Build up several levels with lots of data so a key can have a value in a
        // deep level and a tombstone in a shallower one.
        for i in 0u64..400 {
            let k = format!("key{i:05}");
            e.put(k.as_bytes(), format!("val{i}").as_bytes(), i + 1)
                .await
                .unwrap();
        }
        // Now delete a key that has long since been flushed/compacted into a deep
        // level; its tombstone lands shallow and must not let the deep value win.
        let v = e.latest_version() + 1;
        e.delete(b"key00100", v).await.unwrap();
        // Churn more to drive compactions that fold the tombstone with shallower
        // tables (the deep value table may not be part of every compaction).
        for i in 400u64..700 {
            let k = format!("key{i:05}");
            e.put(k.as_bytes(), format!("val{i}").as_bytes(), i + 2)
                .await
                .unwrap();
        }
        // The key stays deleted no matter how GC folded the levels.
        assert_eq!(
            e.get(b"key00100").await.unwrap(),
            None,
            "seed={seed}: GC must not resurrect a key with a deeper old value"
        );
        // Every other key is intact.
        for i in (0u64..700).filter(|&i| i != 100) {
            let k = format!("key{i:05}");
            assert_eq!(
                e.get(k.as_bytes()).await.unwrap().unwrap().value,
                format!("val{i}").as_bytes(),
                "seed={seed}: key {k} corrupted by GC"
            );
        }
    });
}

/// A live [`Snapshot`] pins the tombstone-GC floor **below its own version**, so
/// compaction must never reclaim a record the snapshot still needs — even long
/// past `tombstone_grace_versions`, and even though the *same* history would be
/// (correctly) reclaimed without the pin, per
/// `aged_tombstone_is_reclaimed_recent_one_is_preserved` above.
#[test]
fn held_snapshot_survives_compaction_gc() {
    let seed = 0x5A0_u64;
    let sim = Simulator::new(seed);
    // Tiny grace: absent the snapshot pin, ordinary churn reclaims this quickly
    // (as the sibling test above demonstrates) — the pin is the only thing
    // protecting it here.
    let e = open(&sim, 1);
    block_on(async {
        // Write the victim, then snapshot immediately: pinned at version 1, so
        // it observes the value *before* the delete below.
        e.put(b"victim", b"hello", 1).await.unwrap();
        let snap = e.snapshot();
        assert_eq!(
            snap.version(),
            1,
            "seed={seed}: snapshot pinned before the delete"
        );
        assert_eq!(
            snap.get(b"victim").await.map(|v| v.value),
            Some(b"hello".to_vec()),
            "seed={seed}: snapshot must see the pre-delete value"
        );

        // Delete it, then churn far past the grace window, forcing compactions
        // that would otherwise reclaim the victim's tombstone *and* the value
        // it shadows (version 1 — exactly what the snapshot is pinned to).
        e.delete(b"victim", 2).await.unwrap();
        for i in 0u64..300 {
            let k = format!("k{i:04}");
            e.put(k.as_bytes(), format!("v{i}").as_bytes(), 100 + i)
                .await
                .unwrap();
        }
        assert!(
            e.compaction_count() >= 1,
            "seed={seed}: expected compactions to run GC"
        );

        // While the snapshot is alive, it must still see the pre-delete value —
        // GC must not have reclaimed version 1 out from under it.
        assert_eq!(
            snap.get(b"victim").await.map(|v| v.value),
            Some(b"hello".to_vec()),
            "seed={seed}: held snapshot's data was reclaimed by GC"
        );
        // A fresh (unpinned) read sees the delete, as expected — the pin
        // protects the *snapshot's* view, not the engine's live state.
        assert_eq!(
            e.get(b"victim").await.unwrap(),
            None,
            "seed={seed}: live read must see the delete"
        );

        // Dropping the snapshot releases its hold on the GC floor — checked
        // directly against the refcount rather than forcing another compaction
        // pass (whether GC actually *revisits* the victim's table next depends
        // on unrelated compaction scheduling; what this test is about is that
        // the pin itself is released, not permanent).
        assert_eq!(e.held_snapshot_count(), 1, "seed={seed}: one snapshot held");
        drop(snap);
        assert_eq!(
            e.held_snapshot_count(),
            0,
            "seed={seed}: dropping the snapshot must release its GC-floor pin"
        );
    });
}
