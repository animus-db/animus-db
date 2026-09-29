//! Issue #1092: a WAL persist round is ONE physical append plus ONE `fsync`,
//! however many log entries it carries.
//!
//! `persist_wal` used to issue one `Disk::append` per drained record and then
//! a single `sync`. On `ProdEnv` that is an `open` + `write` + `flush` per
//! record; on a latency-modelled `SimEnv` disk (`DiskConfig::set_sync_delay`
//! applies to every `append` AND `sync`) it made a follower's persist round
//! for one full `AppendEntries` batch (512 entries,
//! `MAX_APPEND_ENTRIES_BATCH`) cost 513 disk latencies. A joining learner in
//! `learner_snapshot_livelock_under_continuous_writer.rs` (20 ms latency)
//! therefore spent ~10 s of virtual time inside a single round — during
//! which its ack was correctly held back (durable-before-visible) — and the
//! round could outlast the harness's whole final-quarter progress window,
//! seed-dependently (seed `0xfa2b71bc5313f78c`, the failure in issue #1092).
//! Nothing was livelocked; the round was just priced per record.
//!
//! The test: a follower on a slow disk must make a whole burst durable in a
//! handful of disk latencies, not one per entry. Verified red against the
//! per-record append (a 300-entry burst needs ~6 s of virtual time at 20 ms)
//! and green with the single coalesced append.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::nid;
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

const SYNC_DELAY: Duration = Duration::from_millis(20);
const BURST: u64 = 300;
/// A generous bound in disk latencies: election-independent replication plus
/// a handful of coalesced rounds. Per-record appends need `BURST` (300).
const MAX_LATENCIES_TO_DURABLE: u32 = 20;

fn run(seed: u64) {
    let mut sim = Simulator::new(seed);
    let ids = [0u64, 1, 2];
    let nodes: Vec<KvNode> = ids
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                ids.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    sim.run_for(Duration::from_secs(2));
    let l = (0..nodes.len())
        .find(|&i| nodes[i].is_leader())
        .unwrap_or_else(|| panic!("seed={seed:#x}: an initial leader"));
    let f = (l + 1) % nodes.len();

    // Slow the follower's disk only after the group is up (a disk slower than
    // the election timeout is an operational limit, not this defect).
    let mut slow = DiskConfig::default();
    slow.set_sync_delay(SYNC_DELAY);
    sim.set_disk_config_for(nid(ids[f]), slow);

    for i in 0..BURST {
        let res = nodes[l].put(format!("k-{i}").into_bytes(), vec![b'v'; 64]);
        assert!(
            matches!(res, ProposeResult::Accepted { .. }),
            "seed={seed:#x}: write {i} must be locally accepted, got {res:?}"
        );
    }
    // The leader's own last log index (every write above is already appended).
    let target = nodes[l].snapshot_index() + nodes[l].log_len() as u64;
    assert!(
        target > BURST,
        "seed={seed:#x}: leader log holds the whole burst"
    );
    sim.run_for(SYNC_DELAY * MAX_LATENCIES_TO_DURABLE);

    let durable = nodes[f].durable_index();
    assert!(
        durable >= target,
        "seed={seed:#x}: a follower with a {SYNC_DELAY:?} disk had made only {durable} of \
         {target} entries durable after {MAX_LATENCIES_TO_DURABLE} disk latencies — a WAL \
         persist round is being priced per record instead of one append + one fsync \
         (issue #1092)"
    );
}

#[test]
fn a_persist_round_costs_one_append_not_one_per_record() {
    for seed in [0x1092_0001u64, 0x1092_0002, 0x1092_0003] {
        run(seed);
    }
}
