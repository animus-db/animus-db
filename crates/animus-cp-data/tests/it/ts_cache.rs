//! ADR 0018 §2/PR2b: the per-tablet read-timestamp cache + logged read
//! ceiling — the serializability write-push half. The rotation math itself
//! (`low_water` rising when a generation is dropped, over-conservative but
//! sound) is unit-tested directly in `src/ts_cache.rs` against the exact
//! `TsCache` the driver uses; this file proves the **integration**
//! properties: a served read genuinely pushes a subsequent write, the
//! mechanism survives a real leader change under adversarial clock skew (the
//! load-bearing test, with its own negative control), ceiling proposals
//! amortize rather than firing once per read, and a real `RaftKvNode` stays
//! correct once its cache has actually rotated under load.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::hlc::{Hlc, HlcTimestamp};
use animus_cp_data::{KIND_BASE, RaftKvNode, StageOutcome, TxnOutcome};
use animus_env::{Clock, EnvExt, Metric, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::KeyRange;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn leader(nodes: &[KvNode], live: &[usize], seed: u64) -> usize {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(i, n)| live.contains(i) && n.is_leader())
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one leader among {live:?}, got {ls:?} (seed={seed})"
    );
    ls[0]
}

fn put(nodes: &[KvNode], live: &[usize], seed: u64, key: &[u8], value: &[u8]) {
    let l = leader(nodes, live, seed);
    assert!(
        matches!(
            nodes[l].put(key.to_vec(), value.to_vec()),
            ProposeResult::Accepted { .. }
        ),
        "leader {l} rejected a put (seed={seed})"
    );
}

fn ts_of(node: &KvNode, key: &[u8]) -> HlcTimestamp {
    let version = block_on(node.storage().get(&node.physical_key(KIND_BASE, key)))
        .expect("engine read ok")
        .unwrap_or_else(|| panic!("key {key:?} missing"))
        .version;
    animus_cp_data::hlc::unpack(version)
}

fn lin_read(sim: &mut Simulator, node: &KvNode, key: &[u8], budget: Duration) -> Option<Vec<u8>> {
    let slot: Arc<Mutex<Option<Option<Vec<u8>>>>> = Arc::new(Mutex::new(None));
    let n = node.clone();
    let s = Arc::clone(&slot);
    let k = key.to_vec();
    node.env().clone().spawn_task(async move {
        let v = n.linearizable_get(&k).await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock()
        .unwrap()
        .clone()
        .expect("linearizable read did not complete")
}

// ============================================================================
// 1. Write-push: a served read pushes the next write above it.
// ============================================================================

#[test]
fn write_push_after_a_served_read_the_next_write_lands_strictly_above_it() {
    let seed = 0x0009_7571;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    put(&nodes, &[0, 1, 2], seed, b"k", b"v0");
    sim.run_for(Duration::from_secs(1));

    // Serve a linearizable read of k: this mints its own serve ts and, since
    // no ceiling has ever been committed yet (starts at zero), proposes one
    // covering it (`uncertainty_upper(serve_ts)`), so `committed_ceiling()`
    // afterward is a real, upper-bounding proxy for "the highest ts any read
    // just served could have been at."
    // A tight budget, deliberately well under `HLC_MAX_OFFSET` (500ms): a
    // healthy 3-node group's ReadIndex round trip resolves in a handful of
    // heartbeat intervals, and `run_for` always burns its *entire* budget
    // even once idle (the house `SimEnv` gotcha) — a generous budget here
    // would race sim time straight past the pushed-ahead ceiling before the
    // negative control below ever gets to check it.
    assert_eq!(
        lin_read(&mut sim, &nodes[l], b"k", Duration::from_millis(100)),
        Some(b"v0".to_vec())
    );
    let ceiling_after_read = nodes[l].committed_ceiling();
    assert!(
        ceiling_after_read > HlcTimestamp::zero(),
        "sanity: the read must have driven a real ceiling (seed={seed})"
    );

    // Negative control (mirrors test 2's below): prove the push is what
    // saves this write, not coincidental clock advancement — a bare,
    // fresh, un-pushed `Hlc` minting *right now* would land at or below
    // the ceiling `uncertainty_upper` deliberately shifted `HLC_MAX_OFFSET`
    // into the future. If this ever failed, the assertion below would
    // prove nothing (the write would have landed above regardless of
    // `mint_pushed`'s witness-retry branch ever firing).
    let bare_mint =
        Hlc::new(nid(l as u64), Duration::from_millis(500)).mint(sim.env(nid(l as u64)).now());
    assert!(
        bare_mint <= ceiling_after_read,
        "test fixture: a bare mint right now must NOT already exceed the \
         pushed-ahead ceiling, or the write-push retry below proves nothing \
         (bare_mint={bare_mint:?} ceiling={ceiling_after_read:?}, seed={seed})"
    );

    // Overwrite k; its committed ts must strictly exceed the ceiling that
    // now covers the just-served read — only reachable via `mint_pushed`'s
    // witness-retry branch (`RaftKvNode::mint_pushed`, PR2b's newest logic),
    // proven necessary by the negative control just above, not by real time
    // having simply caught up to the ceiling on its own.
    put(&nodes, &[0, 1, 2], seed, b"k", b"v1");
    sim.run_for(Duration::from_secs(1));
    let write_ts = ts_of(&nodes[l], b"k");
    assert!(
        write_ts > ceiling_after_read,
        "a write following a served read must land strictly above the ceiling \
         covering that read (write_ts={write_ts:?} ceiling={ceiling_after_read:?}, seed={seed})"
    );
}

// ============================================================================
// 2. Leader-change safety (load-bearing): a write through a NEW leader must
//    never undercut a read a DEPOSED leader served, even under adversarial
//    clock skew — with a negative control proving the ceiling mechanism,
//    not coincidental clock ordering, is what saves it.
// ============================================================================

#[test]
fn leader_change_never_lets_a_write_undercut_a_served_read_even_under_extreme_clock_skew() {
    let seed = 0x7EA1_0000;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));

    let a = leader(&nodes, &[0, 1, 2], seed);
    // A's clock reads WAY ahead of the shared timeline for the rest of the
    // scenario; the two survivors stay unskewed (i.e. behind A the whole
    // time) — exactly the adversarial shape the write-push/ceiling design
    // must survive.
    sim.set_clock_skew_for(nid(a as u64), 60_000_000_000); // +60s

    put(&nodes, &[0, 1, 2], seed, b"k", b"v0");
    sim.run_for(Duration::from_secs(1));

    // Serve several linearizable reads through A while it's skewed far
    // ahead: each drives its own ceiling forward (committed through the
    // group's real Raft log), so the survivors witness it via ordinary
    // `AppendEntries` receipt — well before A is ever killed.
    for _ in 0..3 {
        assert_eq!(
            lin_read(&mut sim, &nodes[a], b"k", Duration::from_secs(2)),
            Some(b"v0".to_vec())
        );
        sim.run_for(Duration::from_millis(500));
    }
    let a_ceiling = nodes[a].committed_ceiling();
    assert!(a_ceiling > HlcTimestamp::zero(), "sanity (seed={seed})");

    // Kill A (partition it away, mirroring `read_index.rs`'s deposed-leader
    // shape); the two survivors — both unskewed — re-elect among themselves.
    let survivors: Vec<usize> = (0..3).filter(|&i| i != a).collect();
    for &s in &survivors {
        sim.partition_pair(nid(a as u64), nid(s as u64));
    }
    sim.run_for(Duration::from_secs(3));

    let b = leader(&nodes, &survivors, seed);

    // Negative control: prove the skew was genuinely large enough to matter
    // — a totally FRESH, un-witnessed `Hlc` on B's own (unskewed) clock,
    // minting *right now*, would land BELOW A's ceiling. If this assertion
    // ever failed, the rest of this test would prove nothing (the ordinary
    // monotonic-clock case would trivially "work" even with the write-push/
    // ceiling mechanism ripped out).
    let bare_b = Hlc::new(nid(b as u64), Duration::from_millis(500));
    let bare_mint = bare_b.mint(sim.env(nid(b as u64)).now());
    assert!(
        bare_mint < a_ceiling,
        "test fixture: the skew must be large enough that B's own raw \
         clock, absent any witnessing, would mint below A's ceiling — \
         otherwise this test has no teeth (bare={bare_mint:?} ceiling={a_ceiling:?}, seed={seed})"
    );

    // The real system: B writes the same key. Its committed ts must still
    // strictly exceed A's ceiling — B witnessed it via ordinary
    // `AppendEntries` receipt (as a follower, before it ever became
    // candidate/leader), exactly the mechanism `ceiling.rs` documents.
    put(&nodes, &survivors, seed, b"k", b"v1");
    sim.run_for(Duration::from_secs(1));
    let b_write_ts = ts_of(&nodes[b], b"k");
    assert!(
        b_write_ts > a_ceiling,
        "B's write must land strictly above A's served-read ceiling despite \
         B's own raw clock reading behind it — the ceiling mechanism, not \
         clock synchronization, must be what saves it \
         (write_ts={b_write_ts:?} ceiling={a_ceiling:?}, seed={seed})"
    );
}

// ============================================================================
// 3. Ceiling amortization: N sequential reads at close ts produce O(1)
//    ceiling proposals, not O(N).
// ============================================================================

#[test]
fn ceiling_proposals_amortize_over_sequential_reads_at_close_timestamps() {
    let seed = 0x4A07_1234;
    let metrics = animus_env::MetricsHandle::recording();
    let sim = Simulator::new(seed);
    let nodes: Vec<KvNode> = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start_with_metrics(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
                metrics.clone(),
            )
        })
        .collect();
    let mut sim = sim;
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    put(&nodes, &[0, 1, 2], seed, b"k", b"v");
    sim.run_for(Duration::from_secs(1));

    // `N` **sequential** reads (one `.await` after another inside a single
    // spawned task, no artificial gap) driven by one bounded `run_for` call.
    // `Simulator::run_for` always advances the clock to the full deadline
    // once idle (even if the driven future finished sooner — it does not
    // "return early"), so calling it once *per read* with a multi-second
    // budget would itself advance each read's serve ts by seconds, trivially
    // exceeding `HLC_MAX_OFFSET` (500ms) between every pair and defeating the
    // very amortization being tested — driving the whole sequential batch
    // under one `run_for` call is what keeps consecutive reads' serve ts
    // close together, as a real back-to-back workload would.
    //
    // `N` is large and the budget generous **on purpose**, rather than tuned
    // to sit just under one `HLC_MAX_OFFSET` window: with amortization
    // working, proposals scale with elapsed wall time (`budget /
    // HLC_MAX_OFFSET`, roughly one refresh every 500ms), not with `N` at
    // all — so a big `N` over a multi-second budget still yields only a
    // handful of proposals, a comfortable, non-flaky margin under `N`
    // instead of a razor's-edge one.
    const N: u64 = 500;
    let results: Arc<Mutex<Vec<Option<Vec<u8>>>>> = Arc::new(Mutex::new(Vec::new()));
    let n = nodes[l].clone();
    let r = Arc::clone(&results);
    nodes[l].env().clone().spawn_task(async move {
        for _ in 0..N {
            let v = n.linearizable_get(b"k").await;
            r.lock().unwrap().push(v);
        }
    });
    sim.run_for(Duration::from_secs(30));

    let results = results.lock().unwrap().clone();
    assert_eq!(
        results.len(),
        N as usize,
        "all {N} sequential reads must have completed within the drive budget (seed={seed})"
    );
    for (i, v) in results.iter().enumerate() {
        assert_eq!(
            v,
            &Some(b"v".to_vec()),
            "read {i} must see the committed value (seed={seed})"
        );
    }

    let proposals = metrics.get(Metric::CpReadCeilingProposals);
    assert!(
        proposals >= 1,
        "at least the first read must have proposed a ceiling (seed={seed})"
    );
    // A 30s budget over ~500ms ceiling windows is at most ~60 refreshes —
    // nowhere near `N=500` if amortization is genuinely O(elapsed time),
    // not O(reads). A generous but still far-from-N bound keeps this a
    // real regression check, not a coin flip.
    assert!(
        proposals <= 100,
        "ceiling proposals must amortize (roughly one per `HLC_MAX_OFFSET` \
         of wall time, not one per read) over N={N} sequential reads — \
         got {proposals} (seed={seed})"
    );
}

// ============================================================================
// 4. Cache rotation: exceed the size bound through a real `RaftKvNode`.
//    `TsCache` is crate-private, so this file cannot inspect `low_water`
//    directly or isolate rotation from ordinary same-leader clock
//    monotonicity the way `src/ts_cache.rs`'s unit tests do (`rotation_
//    evicts_current_into_previous_and_low_water_never_loses_the_evicted_
//    max`, which drives `TsCache::bump`/`rotate` directly and asserts on
//    its internal state) — that unit suite is the exhaustive proof of the
//    rotation math itself. What an external test *can* prove is the
//    integration wiring: a real node stays correct — no panic, still-live
//    reads and writes still agree — once its cache has genuinely rotated
//    several times over under sustained distinct-key read load. A
//    single-voter group is used so each read barrier resolves at zero
//    simulated cost (majority = 1, self trivially confirms), keeping a
//    many-thousand-read run fast and deterministic.
// ============================================================================

#[test]
fn a_real_node_stays_correct_after_its_cache_has_rotated_several_times_over() {
    let seed = 0x0080_7A7E;
    let mut sim = Simulator::new(seed);
    let id = nid(0);
    let node: KvNode =
        RaftKvNode::start(sim.env(id.clone()), vec![id.clone()], MemoryEngine::new());
    sim.run_for(Duration::from_secs(2)); // elect (single voter)

    put(
        std::slice::from_ref(&node),
        &[0],
        seed,
        b"anchor",
        b"before",
    );
    sim.run_for(Duration::from_millis(50));

    // Enough distinct-key reads to rotate the (crate-private, 4096-entry)
    // cache generation several times over, driven within one spawned task
    // + one bounded `run_for` (see the amortization test's comment on why
    // per-call `run_for` budgets would be the wrong tool here). Each read's
    // key is absent — the cache bumps regardless of whether the read finds
    // a value (see `linearizable_get_served`'s doc), so no writes are
    // needed to build up entries.
    const READS: u64 = 9000;
    let n = node.clone();
    let done = Arc::new(Mutex::new(false));
    let d = Arc::clone(&done);
    node.env().clone().spawn_task(async move {
        for i in 0..READS {
            let key = format!("k{i}").into_bytes();
            let _ = n.linearizable_get(&key).await;
        }
        *d.lock().unwrap() = true;
    });
    sim.run_for(Duration::from_secs(15));
    assert!(
        *done.lock().unwrap(),
        "all {READS} reads must have completed within the drive budget (seed={seed})"
    );

    // The node must still be fully functional: a fresh read of the
    // untouched anchor key still sees its value, and a fresh write still
    // commits and applies correctly. Driven via `lin_read` (spawned task +
    // `run_for`), never a bare `block_on` — a linearizable read's barrier
    // needs the `Simulator` itself to be actively driven to resolve its
    // `.await` points; `block_on` would just block the very thread that
    // would drive it, hanging forever (this crate's own documented gotcha).
    assert_eq!(
        lin_read(&mut sim, &node, b"anchor", Duration::from_secs(2)),
        Some(b"before".to_vec()),
        "a real key must still read correctly after heavy cache churn (seed={seed})"
    );
    put(std::slice::from_ref(&node), &[0], seed, b"anchor", b"after");
    sim.run_for(Duration::from_millis(50));
    assert_eq!(
        lin_read(&mut sim, &node, b"anchor", Duration::from_secs(2)),
        Some(b"after".to_vec()),
        "a write must still commit/apply correctly after heavy cache churn (seed={seed})"
    );
}

// ============================================================================
// 5. Clock-divergence regression: the `mint_pushed` clock-witnessing
//    runaway (ADR 0018 §2 amendment). `mint_pushed` used to fold the LIVE
//    committed ceiling into every write's floor and, whenever the honest
//    mint fell short of it (the ceiling is deliberately `HLC_MAX_OFFSET`
//    ahead of real time), witness that floor — permanently ratcheting the
//    group's shared `Hlc` into the fiction. The next read then mints near
//    that poisoned clock, exceeds the (now-stale) ceiling almost at once,
//    forcing a fresh one `HLC_MAX_OFFSET` further out, which floors the
//    next write, and so on: a k*HLC_MAX_OFFSET runaway lattice that grows
//    roughly one `HLC_MAX_OFFSET` per round regardless of how much real
//    (virtual) time actually elapses. Per-term ceiling absorption (fold the
//    ceiling in at most once per term, not on every mint) plus the
//    no-witness push (bump strictly above the floor as pure arithmetic,
//    never through `Hlc::witness`) close this. This test is deliberately
//    interleaved reads-then-writes on a tight loop — exactly the shape that
//    drives the feedback loop — and is a genuine `SimEnv` regression: the
//    bug is a logic error in how the clock advances, not a real-thread
//    timing race, so simulation catches it byte-for-byte.
// ============================================================================

#[test]
fn interleaved_reads_and_writes_never_let_minted_timestamps_outrun_real_time() {
    // Mirrors `HLC_MAX_OFFSET`'s own value (private to the crate) — see this
    // file's other tests for the same hardcoded-500ms convention.
    const HLC_MAX_OFFSET_MS: u64 = 500;
    let seed = 0x7ED0_C1DE;
    let mut sim = Simulator::new(seed);
    let id = nid(0);
    let node: KvNode =
        RaftKvNode::start(sim.env(id.clone()), vec![id.clone()], MemoryEngine::new());
    sim.run_for(Duration::from_secs(2)); // elect (single voter)

    put(std::slice::from_ref(&node), &[0], seed, b"k", b"v0");
    sim.run_for(Duration::from_millis(50));
    let start_ms = sim.env(id.clone()).now().0 / 1_000_000;

    // Tight, back-to-back rounds of a linearizable read (which observes/
    // proposes the read ceiling) immediately followed by a write (which
    // mints through `mint_pushed`) — precisely the read-pushes-ceiling /
    // write-folds-and-witnesses-it-forward / next-read-mints-near-the-
    // poisoned-clock cycle the bug produces. A single-voter group keeps
    // every read barrier resolving at negligible simulated cost (mirroring
    // test 4's rationale), so real elapsed time stays governed by
    // `run_for`'s budget, not by network round trips — the clean baseline
    // the divergence bound below needs.
    const ROUNDS: u64 = 200;
    let n = node.clone();
    let done = Arc::new(Mutex::new(false));
    let d = Arc::clone(&done);
    node.env().clone().spawn_task(async move {
        for i in 0..ROUNDS {
            let _ = n.linearizable_get(b"k").await;
            let value = format!("v{i}").into_bytes();
            let _ = n.put(b"k".to_vec(), value);
        }
        *d.lock().unwrap() = true;
    });
    let budget = Duration::from_secs(20);
    sim.run_for(budget);
    assert!(
        *done.lock().unwrap(),
        "all {ROUNDS} interleaved read/write rounds must complete within the drive budget \
         (seed={seed})"
    );

    // `run_for` always burns its entire budget (the house `SimEnv` gotcha
    // this file's other tests already document), so real elapsed time here
    // is governed by `budget`, not by how long the loop above actually took.
    let elapsed_ms = (sim.env(id.clone()).now().0 / 1_000_000).saturating_sub(start_ms);
    let ceiling_ms = node.committed_ceiling().wall_ms;
    let write_ms = ts_of(&node, b"k").wall_ms;

    // Neither the committed read ceiling nor the last write's own minted
    // timestamp may run away from real elapsed time. The only legitimate
    // margin is a small, bounded number of `HLC_MAX_OFFSET` windows (the
    // ceiling is always proposed one `HLC_MAX_OFFSET` ahead of the read it
    // covers, amortizing to roughly one refresh per window under sustained
    // load — see test 3 above); anything beyond that means the
    // clock-witnessing feedback loop is running again.
    let bound_ms = elapsed_ms + 3 * HLC_MAX_OFFSET_MS;
    assert!(
        ceiling_ms <= bound_ms,
        "committed ceiling ran away from real time: ceiling={ceiling_ms}ms \
         elapsed={elapsed_ms}ms bound={bound_ms}ms (seed={seed}) — the mint_pushed \
         clock-witnessing runaway is back"
    );
    assert!(
        write_ms <= bound_ms,
        "minted write timestamp ran away from real time: write={write_ms}ms \
         elapsed={elapsed_ms}ms bound={bound_ms}ms (seed={seed}) — the mint_pushed \
         clock-witnessing runaway is back"
    );
}

// ============================================================================
// 6. Cross-group transaction clock-divergence regression (ADR 0018 §2
//    write-loss amendment, Part A). Bug 3: a multi-participant transaction's
//    `commit_ts` is minted on the ANCHOR's own group (possibly pushed up to
//    `HLC_MAX_OFFSET` ahead of real time by that group's own read-conflict
//    push, exactly test 5's mechanism, now legal and bounded post-PR1) and
//    travels inside `TxnResolve`'s payload to every PARTICIPANT — a
//    genuinely foreign timestamp arriving somewhere the pre-fix witnessing
//    chain never looked (see `RaftKvNode::txn_resolve`'s apply arm in
//    `src/lib.rs`). Witnessing it there is legitimate HLC causality, not a
//    repeat of test 5's outlawed fictional-ceiling witnessing — but any new
//    witness call is exactly the shape of change that historically
//    re-introduced a runaway (test 5 itself exists because an *earlier*
//    witness call did), so this is the same proof test 5 runs, aimed at the
//    new call site: two independent groups, repeated back-to-back
//    cross-group transactions (each one exercising the anchor's
//    `mint_at_least` push AND the participant's new `TxnResolve` witness)
//    interleaved with linearizable reads on both sides (to keep each
//    group's own read ceiling churning too), asserting BOTH groups' clocks
//    stay within a small, ROUND-COUNT-INDEPENDENT bound of real elapsed
//    time — the signature of a bounded, one-time transfer rather than a
//    compounding feedback loop. Deliberately a plain in-test 2PC (staging/
//    committing/resolving directly on the two handles, mirroring
//    `tests/txn_multi.rs::run_txn`'s shape) rather than pulling in that
//    file's coordinator, since this file's own single-voter-group,
//    virtual-time-elapsed style (tests 1-5 above) is what the divergence
//    bound needs.
// ============================================================================

#[test]
fn cross_group_txn_traffic_never_lets_either_groups_clock_run_away() {
    // Mirrors test 5's own hardcoded-500ms convention.
    const HLC_MAX_OFFSET_MS: u64 = 500;
    let seed = 0xC205_50FF;
    let mut sim = Simulator::new(seed);
    let id_a = nid(100);
    let id_b = nid(200);
    let anchor: KvNode = RaftKvNode::start(
        sim.env(id_a.clone()),
        vec![id_a.clone()],
        MemoryEngine::new(),
    );
    let participant: KvNode = RaftKvNode::start(
        sim.env(id_b.clone()),
        vec![id_b.clone()],
        MemoryEngine::new(),
    );
    sim.run_for(Duration::from_secs(2)); // elect (single voter, both groups)

    // Every real data-plane key leads with an 8-byte partition token (ADR
    // 0022) — `txn_stage_anchor`'s own assert requires it.
    let a_key = {
        let mut k = vec![1u8; 8];
        k.extend_from_slice(b"a");
        k
    };
    let b_key = {
        let mut k = vec![2u8; 8];
        k.extend_from_slice(b"b");
        k
    };
    put(std::slice::from_ref(&anchor), &[0], seed, &a_key, b"a0");
    put(
        std::slice::from_ref(&participant),
        &[0],
        seed,
        &b_key,
        b"b0",
    );
    sim.run_for(Duration::from_millis(50));
    let start_ms = sim.env(id_a.clone()).now().0 / 1_000_000;

    const ROUNDS: u64 = 80;
    let a = anchor.clone();
    let b = participant.clone();
    let ak = a_key.clone();
    let bk = b_key.clone();
    let done = Arc::new(Mutex::new(false));
    let d = Arc::clone(&done);
    anchor.env().clone().spawn_task(async move {
        for i in 0..ROUNDS {
            // Reads on both sides, exactly like test 5, to keep pushing
            // each group's own read ceiling independently.
            let _ = a.linearizable_get(&ak).await;
            let _ = b.linearizable_get(&bk).await;

            // A minimal two-participant 2PC — stage anchor + participant,
            // commit on the anchor (`mint_at_least`, which already
            // legitimately witnesses every participant's own acked stage
            // ts), then resolve both (the participant's resolve is Part
            // A's new witness call site).
            let value = format!("v{i}").into_bytes();
            let Some((txn_id, record_key, outcome)) = a
                .txn_stage_anchor(
                    "t",
                    vec![animus_cp_data::TxnWrite::plain(
                        ak.clone(),
                        Some(value.clone()),
                    )],
                    vec![("t".to_string(), KeyRange::new(bk.clone(), None))],
                    Vec::new(),
                )
                .await
            else {
                continue;
            };
            if outcome != StageOutcome::Staged {
                continue;
            }
            let Some((participant_ts, p_outcome)) = b
                .txn_stage_participant(
                    txn_id.clone(),
                    record_key.clone(),
                    "t".to_string(),
                    vec![animus_cp_data::TxnWrite::plain(
                        bk.clone(),
                        Some(value.clone()),
                    )],
                    Vec::new(),
                )
                .await
            else {
                continue;
            };
            if p_outcome != StageOutcome::Staged {
                continue;
            }
            let candidate = txn_id.ts.max(participant_ts);
            let Some(commit_ts) = a
                .txn_commit_at_least(txn_id.clone(), record_key.clone(), candidate)
                .await
            else {
                continue;
            };
            let commit_outcome = TxnOutcome::Committed { commit_ts };
            let _ = a
                .txn_resolve(
                    txn_id.clone(),
                    record_key.clone(),
                    vec![ak.clone()],
                    commit_outcome.clone(),
                )
                .await;
            let _ = b
                .txn_resolve(
                    txn_id.clone(),
                    record_key.clone(),
                    vec![bk.clone()],
                    commit_outcome,
                )
                .await;
        }
        *d.lock().unwrap() = true;
    });
    let budget = Duration::from_secs(30);
    sim.run_for(budget);
    assert!(
        *done.lock().unwrap(),
        "all {ROUNDS} cross-group txn rounds must complete within the drive budget (seed={seed})"
    );

    // Same divergence-bound style as test 5: real elapsed time is governed
    // by `budget` (`run_for` always burns it), and the only legitimate
    // margin is a small, fixed number of `HLC_MAX_OFFSET` windows —
    // independent of `ROUNDS` — covering: the anchor's own read-ceiling
    // push, the participant's own read-ceiling push, and one bounded
    // transfer of the anchor's commit-ts lead into the participant via
    // Part A's witness. A bound that held regardless of `ROUNDS` here is
    // exactly what distinguishes a one-time transfer from a reignited
    // runaway (which would compound with every round instead).
    let elapsed_ms = (sim.env(id_a.clone()).now().0 / 1_000_000).saturating_sub(start_ms);
    let bound_ms = elapsed_ms + 5 * HLC_MAX_OFFSET_MS;

    let a_ceiling_ms = anchor.committed_ceiling().wall_ms;
    let b_ceiling_ms = participant.committed_ceiling().wall_ms;
    let a_write_ms = ts_of(&anchor, &a_key).wall_ms;
    let b_write_ms = ts_of(&participant, &b_key).wall_ms;
    for (label, ms) in [
        ("anchor ceiling", a_ceiling_ms),
        ("participant ceiling", b_ceiling_ms),
        ("anchor last write", a_write_ms),
        ("participant last write", b_write_ms),
    ] {
        assert!(
            ms <= bound_ms,
            "{label} ran away from real time: {label}={ms}ms elapsed={elapsed_ms}ms \
             bound={bound_ms}ms (seed={seed}) — either the mint_pushed clock-witnessing \
             runaway (test 5) is back, or the new cross-group TxnResolve witness (ADR 0018 §2 \
             write-loss amendment, Part A) has reignited it on the participant side"
        );
    }
}
