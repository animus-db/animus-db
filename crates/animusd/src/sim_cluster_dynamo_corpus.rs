//! The DynamoDB-wire cycles/durability corpus (ADR 0061 rung D2 PR 2, C-04
//! D2 step 3): the actual end-to-end wire corpus D2 PR 1
//! (`sim_cluster_dynamo.rs`'s own module doc) named as its own remaining
//! work — every op issued as a real DynamoDB JSON request through
//! [`SimClusterHandle::dynamo`], decoded back into the shared
//! [`animus_test::history`] `Mop`/`History` model so
//! `check_cycles`/`check_durability`/`check_convergence` run **unchanged**,
//! mirroring `sim_cluster_corpus.rs`'s own architecture exactly (see that
//! file's module doc for the shared design this one reuses verbatim: the
//! `SimClusterHandle`-over-`&self` split that makes a concurrent workload
//! possible at all, the single-writer-per-key discipline, the
//! `Key`-embeds-table-index convention for `two_tables`, and the
//! converged-or-timeout durability/convergence poll).
//!
//! # The list-append mapping: `UpdateItem`'s `list_append`, not a
//! client-tracked list
//!
//! Unlike `sim_cluster_corpus.rs` (which tracks each client's own list
//! locally and `put`s the whole encoded list on every write — sound only
//! because a raw `put` is a plain, idempotent overwrite), this corpus's one
//! write mechanism is a **server-evaluated** `UpdateItem`:
//!
//! ```text
//! SET items = list_append(if_not_exists(items, :empty), :v)
//! ```
//!
//! with `:v` a one-element list holding this write's own globally-unique
//! value (`Shared::fresh_value`) and `:empty` an empty list, so the very
//! first write to a key needs no separate provisioning step
//! (`if_not_exists` supplies the seed). This single expression is both the
//! `SET` clause and the `list_append` function call the task brief asks
//! for, **and** it is the corpus's genuinely non-idempotent operation: the
//! server reads the item's *current* list at apply time and appends to it
//! — a retried/duplicated apply of the same entry would append the same
//! value twice, unlike a plain `Put`/`SET x = :v`, which a duplicate apply
//! leaves byte-identical. No separate `ADD` op is needed to get this
//! property; `list_append` already has it by construction, which is why
//! the checker's `info`-not-`fail` discipline for an indeterminate outcome
//! genuinely matters here (see `run_write`'s own doc).
//!
//! # The read-consistency modeling decision (ADR 0055)
//!
//! `GetItem`/`Query`/`Scan` all decode `ConsistentRead`, and this corpus
//! issues both values. **`ConsistentRead: true` reads feed the shared
//! `Recorder`/`History` `check_cycles` runs against** — a linearizable
//! ReadIndex read is a real, ordered observation, exactly the discipline
//! `sim_cluster_corpus.rs`'s own read helper documents ("a read that
//! verifies a write must ask for `ConsistentRead: true`",
//! `crates/animusd/CLAUDE.md`'s ADR 0055 testing-gotcha entry).
//! **`ConsistentRead: false` reads are deliberately EXCLUDED from
//! `check_cycles`'s history** — feeding a replica-local, un-barriered
//! observation into the same `wr`/`rw` graph a linearizable read builds
//! would manufacture false-positive "divergence" violations the instant a
//! stale-but-legal read landed during a fault window, exactly the trap
//! `check_cycles`'s own `recover` doc names for a workload-modeling
//! mismatch (a legitimately weaker read is not the same defect class as a
//! forked/stale *strong* read). Instead, every `ConsistentRead: false`
//! observation is recorded separately
//! (`Shared::eventual_reads`) and checked directly against the scenario's
//! own **converged final state** once the fault schedule has healed and
//! drained: `check_eventual_reads_are_prefixes` asserts each observed list
//! is a prefix of the final state — sound under this corpus's
//! single-writer-per-key discipline, since a lagging/un-barriered replica
//! can only ever have observed an *earlier* state of the one writer's own
//! strictly-ordered commit sequence, never a value out of order or a value
//! that never committed. This is the "record them with the checker's own
//! weaker-read flag" fork the task brief poses, resolved as "no such flag
//! exists on the shared, cross-crate `check_cycles` — exclude and check
//! against convergence instead," the identical choice
//! `sim_cluster_corpus.rs` already made for `delete` (see that file's own
//! "delete is exercised, but deliberately kept OUT of the Elle model"
//! section) — a workload-shape exclusion from a checker whose model
//! doesn't fit, not a defect in either the workload or the checker.
//!
//! # `DeleteItem`/`BatchWriteItem`: exercised, kept out of `check_cycles`
//!
//! Both share `sim_cluster_corpus.rs`'s own `delete` reasoning: a
//! tombstoning `DeleteItem` cannot satisfy the list-append prefix
//! invariant (a deleted-then-reappended key's later reads are legitimately
//! not a superset of an earlier one), and a `BatchWriteItem` `PutRequest`
//! is a **whole-item overwrite**, not an append — feeding either into the
//! shared list-append history would manufacture the same false-positive
//! divergence the module doc above already explains for eventual reads.
//! Both get their own **direct** correctness probes instead
//! ([`run_delete_probe`]/[`run_batch_write_probe`]), run from every node in
//! the cluster in turn after the scenario's fault schedule has healed and
//! drained — mirroring `sim_cluster_corpus.rs`'s own `run_delete_probe`
//! exactly, on dedicated key namespaces (`delete-probe-*`/
//! `batch-probe-*`) disjoint from the list-append model's own
//! `part-{0,1}`/`item-{k}` keys, so neither probe's writes can ever be
//! mistaken for (or corrupt) a modeled key.
//!
//! # Multi-key reads: `Query`/`Scan` feed the SAME history, as multiple
//! `Mop::Read`s per transaction
//!
//! Every table's items live under exactly [`PARTITIONS`] fixed partition
//! keys (`part-0`/`part-1`), each holding every item whose logical key
//! hashes to it — so a base-table `Query` (`KeyConditionExpression: pk =
//! :p`) returns a real, strict subset of the table (proving
//! `dispatch_item_op`'s base-table `Query` arm, not merely a synonym for
//! `Scan`), and a `Scan` returns the whole table. Both are always issued
//! `ConsistentRead: true` (Query/Scan's own consistency-mode coverage is
//! not this corpus's concern — `GetItem`'s dedicated true/false split
//! above already covers that dimension) and decode every returned item's
//! own `items` list into one [`Mop::Read`] per item, all recorded as one
//! history entry (`Recorder::ok` already takes a `Vec<Mop>` for exactly
//! this shape) — `check_cycles`'s `wr`/`rw` edges fall out unchanged for a
//! multi-key read exactly as they do for `raftkv_linearizable.rs`'s own
//! transactional workloads.
//!
//! # The cells
//!
//! Identical to `sim_cluster_corpus.rs`'s own 8 (`baseline`,
//! `leader_crash`, `follower_crash`, `stop_restart`, `leader_partition`,
//! `split_brain`, `forward_heavy`, `two_tables`) — same `Nemesis`
//! semantics, same cluster shapes, reused rather than reinvented since the
//! fault dimension this corpus exercises is identical; only the workload
//! riding on top changed from raw KV ops to DynamoDB wire ops.
//!
//! # Depth knob
//!
//! `ANIMUS_DYNAMO_WIRE_SEEDS` (default 1 = the 8 cells above,
//! byte-identical to the committed set) — `corpus::seed_expand` over
//! [`corpus_cells`], the same house convention every other corpus in this
//! workspace uses. Run via `cargo test -p animusd --lib
//! sim_cluster_dynamo_corpus`.
//!
//! # Shrink wiring (ADR 0061 rung B4)
//!
//! Mirrors `sim_cluster_corpus.rs`'s own wiring exactly: [`Scenario`]/
//! [`Nemesis`] derive `Serialize`/`Deserialize`, [`scenario_candidates`]
//! reduces `faults`/`window`/`rounds`/`keyspace`/`clients` (never `name`/
//! `seed`/`nodes`/`replication`/`tables`), [`shrink_and_report`] runs under
//! `ANIMUS_SHRINK=1` only after a scenario is already known to have
//! failed, and `sim_cluster_dynamo_shrink_replay` (`#[ignore]`d) reads
//! `ANIMUS_SHRINK_REPLAY` to re-run a printed minimized case.
//!
//! # Residuals (out of scope for this rung — see `dynamo.rs`'s own
//! `dispatch_item_op` doc for the full "what's ProdEnv-only, why" account)
//!
//! GSI/LSI `Query`/`Scan`, `TransactWriteItems`/`TransactGetItems`, and
//! PartiQL (`ExecuteStatement`/`BatchExecuteStatement`/
//! `ExecuteTransaction`) are all still unreachable through the generic
//! `dispatch_item_op` core this corpus drives — `execute_item_op_as`
//! returns a clean `InternalServerError` for any of them, so this corpus
//! never issues one. Deferred to whichever rung generalizes those
//! operations next (D3/D4 per the ADR's own roadmap), not attempted here.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use animus_env::{Clock, EnvExt, Rng};
use animus_item::AttributeValue;
use animus_sim::SimEnv;
use animus_test::corpus::{self, SeedVariant};
use animus_test::history::{Key, Mop, Process};
use animus_test::shrink::{self, ShrinkReport};
use animus_test::{CheckReport, Recorder, check_convergence, check_cycles, check_durability};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::sim_cluster::{SimCluster, SimClusterHandle};
use super::*;

/// Settle time before the workload starts — mirrors `sim_cluster_corpus.
/// rs`'s identical `SETTLE`.
const SETTLE: Duration = Duration::from_millis(300);
/// Inter-round gap so client tasks interleave rather than lock-stepping.
const POLL: Duration = Duration::from_millis(80);
/// How long the runner holds a scheduled fault's own outage window open
/// before healing.
const FAULT_WINDOW: Duration = Duration::from_millis(1200);
/// Post-heal drain: run the workload tail to completion before taking the
/// history snapshot the `cycles` verdict is checked against.
const DRAIN: Duration = Duration::from_secs(6);
/// Converged-or-timeout poll step + budget for the durability/convergence
/// checks.
const CONVERGENCE_POLL_STEP: Duration = Duration::from_secs(1);
const CONVERGENCE_BUDGET: Duration = Duration::from_secs(15);

/// A `Key`'s high-order digits name which table it belongs to — see
/// `sim_cluster_corpus.rs`'s own identical constant/note for why (multiple
/// tables sharing one `Key` space without conflating their histories).
const TABLE_KEY_STRIDE: Key = 1_000_000;

/// Every table's items live under exactly this many fixed partition keys
/// (`part-0`, `part-1`, …) — small and fixed so a base-table `Query`
/// (scoped to one partition) returns a real, strict subset of a `Scan`
/// (the whole table), giving the two operations genuinely different
/// coverage rather than one being a synonym for the other.
const PARTITIONS: u64 = 2;

/// Split `key` back into `(table name, partition key, sort key)` — the
/// wire-request identity for this fixture's own `(pk, sk)` composite
/// schema. `logical % PARTITIONS` picks a fixed partition; `item-{logical}`
/// is the sort key, parsed back by [`key_from_table_and_sk`].
fn table_pk_sk(key: Key) -> (String, String, String) {
    let table = key / TABLE_KEY_STRIDE;
    let logical = key % TABLE_KEY_STRIDE;
    (
        format!("t{table}"),
        format!("part-{}", logical % PARTITIONS),
        format!("item-{logical}"),
    )
}

/// The inverse of [`table_pk_sk`]'s `sk` half, given the table index a
/// `Query`/`Scan` caller already knows (it built the request for that
/// table) — recovers a full `Key` from one returned item's own `sk`
/// attribute. `None` for a row this corpus didn't write itself (should
/// never happen against a table only this corpus's own workload touches,
/// but a probe's own disjoint `delete-probe-*`/`batch-probe-*` keys must
/// never be mistaken for a modeled one if a caller ever queried the same
/// table — this corpus keeps every probe on its own partition-free key
/// shape specifically so this parse fails cleanly on them instead of
/// silently aliasing onto a modeled `Key`).
fn key_from_table_and_sk(table: u64, sk: &str) -> Option<Key> {
    let logical: u64 = sk.strip_prefix("item-")?.parse().ok()?;
    Some(table * TABLE_KEY_STRIDE + logical)
}

/// Build the DynamoDB JSON value for a one-element list holding `value`
/// (`{"L": [{"N": "value"}]}`) — the `:v` operand of every append's
/// `UpdateExpression`.
fn one_element_list(value: u64) -> Value {
    json!({"L": [{"N": value.to_string()}]})
}

/// Decode an already-parsed item's `items` attribute (DynamoDB wire JSON:
/// `{"items": {"L": [{"N": "1"}, {"N": "2"}, ...]}}`) into the plain
/// `Vec<u64>` the shared checker model wants — absent/malformed reads as
/// an empty list, the same "absent key ⇒ `Some(vec![])`" convention
/// `sim_cluster_corpus.rs`'s own `run_read` uses for a `None` value.
fn decode_items_attr(item: &Value) -> Vec<u64> {
    item.get("items")
        .and_then(|v| v.get("L"))
        .and_then(Value::as_array)
        .map(|elems| {
            elems
                .iter()
                .filter_map(|e| e.get("N")?.as_str()?.parse::<u64>().ok())
                .collect()
        })
        .unwrap_or_default()
}

/// Decode the raw stored bytes at one engine key (`SimClusterHandle::
/// local_value`'s own return shape — this corpus's *internal* item codec,
/// `animus_item::decode_stored_item`, not the DynamoDB wire JSON
/// `decode_items_attr` above parses) into the same `Vec<u64>` shape —
/// used only by [`final_state`], the direct local-engine read every other
/// corpus in this crate uses for its own durability/convergence snapshot.
fn decode_engine_items(bytes: &[u8]) -> Vec<u64> {
    let Ok(Some(item)) = animus_item::decode_stored_item(bytes) else {
        return Vec::new();
    };
    match item.get("items") {
        Some(AttributeValue::L(list)) => list
            .iter()
            .filter_map(|v| match v {
                AttributeValue::N(s) => s.parse::<u64>().ok(),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Declarative scenario model — identical shape to `sim_cluster_corpus.rs`.
// ---------------------------------------------------------------------------

/// A fault the runner injects once, at a scheduled offset from the start of
/// the workload — identical semantics to `sim_cluster_corpus.rs`'s own
/// `Nemesis` (kept as its own copy, per this workspace's "each corpus's own
/// workload-adjacent details are a private copy" convention — see that
/// file's module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Nemesis {
    /// Crash the tablet's current leader.
    LeaderCrash,
    /// Crash a non-leader replica of the tablet.
    FollowerCrash,
    /// A true process restart of the tablet's current leader.
    StopRestart,
    /// Partition the tablet's current leader away from every other node.
    LeaderPartition,
    /// Partition the WHOLE cluster into two non-empty halves.
    SplitBrain,
}

impl Nemesis {
    fn apply(self, cluster: &mut SimCluster, tablet: TabletId) {
        match self {
            Nemesis::LeaderCrash => {
                if let Some(leader) = cluster.leader_index_of(tablet) {
                    cluster.crash(leader);
                }
            }
            Nemesis::FollowerCrash => {
                if let Some(leader) = cluster.leader_index_of(tablet) {
                    let replicas = cluster.handle().replicas_of(tablet);
                    if let Some(&follower) = replicas.iter().find(|&&n| n != leader) {
                        cluster.crash(follower);
                    }
                }
            }
            Nemesis::StopRestart => {
                let victim = cluster.leader_index_of(tablet).unwrap_or(0);
                cluster.restart(victim);
            }
            Nemesis::LeaderPartition => {
                if let Some(leader) = cluster.leader_index_of(tablet) {
                    for n in 0..cluster.node_count() as u64 {
                        if n != leader {
                            cluster.partition(leader, n);
                        }
                    }
                }
            }
            Nemesis::SplitBrain => {
                let n = cluster.node_count() as u64;
                let half = n.div_ceil(2);
                for a in 0..half {
                    for b in half..n {
                        cluster.partition(a, b);
                    }
                }
            }
        }
    }
}

/// A seed-reproducible scenario — the DynamoDB-wire twin of
/// `sim_cluster_corpus.rs`'s own `Scenario`, same fields, same meaning.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Scenario {
    name: String,
    seed: u64,
    nodes: usize,
    replication: usize,
    tables: usize,
    clients: usize,
    rounds: u64,
    keyspace: u64,
    read_pct: u64,
    faults: Vec<(Duration, Nemesis)>,
    window: Duration,
}

impl SeedVariant for Scenario {
    fn scenario_name(&self) -> &str {
        &self.name
    }
    fn reseeded(&self, name: String, seed: u64) -> Self {
        Scenario {
            name,
            seed,
            ..self.clone()
        }
    }
}

fn base_workload(
    name: &str,
    nodes: usize,
    replication: usize,
    tables: usize,
    faults: Vec<(Duration, Nemesis)>,
    window: Duration,
) -> Scenario {
    Scenario {
        seed: corpus::name_seed(name),
        name: name.to_owned(),
        nodes,
        replication,
        tables,
        clients: 3,
        rounds: 6,
        keyspace: 3,
        read_pct: 40,
        faults,
        window,
    }
}

/// The 8 named cells this corpus ships with — identical fault matrix to
/// `sim_cluster_corpus.rs`'s own [`corpus_cells`] (see this file's module
/// doc's "The cells" section).
fn corpus_cells() -> Vec<Scenario> {
    const FAULT_AT: Duration = Duration::from_millis(900);
    vec![
        base_workload("dynamowire_baseline", 3, 3, 1, vec![], Duration::ZERO),
        base_workload(
            "dynamowire_leader_crash",
            3,
            3,
            1,
            vec![(FAULT_AT, Nemesis::LeaderCrash)],
            FAULT_WINDOW,
        ),
        base_workload(
            "dynamowire_follower_crash",
            3,
            3,
            1,
            vec![(FAULT_AT, Nemesis::FollowerCrash)],
            FAULT_WINDOW,
        ),
        base_workload(
            "dynamowire_stop_restart",
            3,
            3,
            1,
            vec![(FAULT_AT, Nemesis::StopRestart)],
            FAULT_WINDOW,
        ),
        base_workload(
            "dynamowire_leader_partition",
            3,
            3,
            1,
            vec![(FAULT_AT, Nemesis::LeaderPartition)],
            FAULT_WINDOW,
        ),
        base_workload(
            "dynamowire_split_brain",
            3,
            3,
            1,
            vec![(FAULT_AT, Nemesis::SplitBrain)],
            FAULT_WINDOW,
        ),
        base_workload("dynamowire_forward_heavy", 4, 2, 1, vec![], Duration::ZERO),
        base_workload("dynamowire_two_tables", 3, 3, 2, vec![], Duration::ZERO),
    ]
}

fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_DYNAMO_WIRE_SEEDS")
}

fn corpus() -> Vec<Scenario> {
    corpus::seed_expand(corpus_cells(), seeds_per_cell())
}

// ---------------------------------------------------------------------------
// The workload.
// ---------------------------------------------------------------------------

struct Shared {
    rec: Mutex<Recorder>,
    next_value: Mutex<u64>,
    /// Acked-write count per issuing node — the `forward_heavy` non-vacuity
    /// signal, identical to `sim_cluster_corpus.rs`'s own field.
    ok_writes_by_node: Mutex<BTreeMap<u64, usize>>,
    /// Every `ConsistentRead: false` observation this scenario made:
    /// `(key, observed list)` — checked after the scenario converges (see
    /// the module doc's "read-consistency modeling decision"), never fed
    /// into the shared `Recorder`/`check_cycles` history.
    eventual_reads: Mutex<Vec<(Key, Vec<u64>)>>,
}

impl Shared {
    fn fresh_value(&self) -> u64 {
        let mut v = self.next_value.lock().expect("next_value poisoned");
        *v += 1;
        *v
    }

    fn record_ok_write(&self, node: u64) {
        *self
            .ok_writes_by_node
            .lock()
            .expect("ok_writes_by_node poisoned")
            .entry(node)
            .or_default() += 1;
    }

    fn record_eventual_read(&self, key: Key, observed: Vec<u64>) {
        self.eventual_reads
            .lock()
            .expect("eventual_reads poisoned")
            .push((key, observed));
    }
}

/// Which flavor of read a round picked — see the module doc's "read-
/// consistency modeling decision" and "multi-key reads" sections for why
/// each is handled differently by [`run_get`]/[`run_query`]/[`run_scan`].
#[derive(Clone, Copy, Debug)]
enum ReadKind {
    ConsistentGet,
    EventualGet,
    Query,
    Scan,
}

/// Append `value` to `key`'s list via a real `UpdateItem` wire request —
/// see the module doc's own "The list-append mapping" section for the
/// exact `UpdateExpression` and why it is the corpus's one genuinely
/// non-idempotent write. `Ok`/`info`, never `fail` — a non-200 response is
/// indeterminate (the write may have applied and only the *reply* was
/// lost/timed out), exactly `sim_cluster_corpus.rs`'s own `run_write`
/// discipline.
async fn run_write(
    env: &SimEnv,
    handle: &SimClusterHandle,
    shared: &Arc<Shared>,
    proc: Process,
    key: Key,
    node: u64,
) {
    let value = shared.fresh_value();
    let (table, pk, sk) = table_pk_sk(key);
    let mops = vec![Mop::Append { key, value }];
    shared
        .rec
        .lock()
        .expect("recorder poisoned")
        .invoke(proc, env.now().0, mops.clone());

    let body = json!({
        "TableName": table,
        "Key": {"pk": {"S": pk}, "sk": {"S": sk}},
        "UpdateExpression": "SET items = list_append(if_not_exists(items, :empty), :v)",
        "ExpressionAttributeValues": {
            ":empty": {"L": []},
            ":v": one_element_list(value),
        },
    })
    .to_string();
    let (status, _body) = handle
        .dynamo(node, "DynamoDB_20120810.UpdateItem", body.as_bytes())
        .await;

    let mut rec = shared.rec.lock().expect("recorder poisoned");
    if status == 200 {
        rec.ok(proc, env.now().0, mops);
        drop(rec);
        shared.record_ok_write(node);
    } else {
        rec.info(proc, env.now().0, mops);
    }
}

/// A single-key `GetItem`, at either consistency level — see the module
/// doc's "read-consistency modeling decision" for why only the
/// `ConsistentRead: true` path feeds the shared history.
async fn run_get(
    env: &SimEnv,
    handle: &SimClusterHandle,
    shared: &Arc<Shared>,
    proc: Process,
    key: Key,
    kind: ReadKind,
    node: u64,
) {
    let (table, pk, sk) = table_pk_sk(key);
    let consistent = matches!(kind, ReadKind::ConsistentGet);
    if consistent {
        shared.rec.lock().expect("recorder poisoned").invoke(
            proc,
            env.now().0,
            vec![Mop::Read {
                key,
                observed: None,
            }],
        );
    }

    let body = json!({
        "ConsistentRead": consistent,
        "TableName": table,
        "Key": {"pk": {"S": pk}, "sk": {"S": sk}},
    })
    .to_string();
    let (status, body) = handle
        .dynamo(node, "DynamoDB_20120810.GetItem", body.as_bytes())
        .await;

    if status != 200 {
        if consistent {
            shared.rec.lock().expect("recorder poisoned").info(
                proc,
                env.now().0,
                vec![Mop::Read {
                    key,
                    observed: None,
                }],
            );
        }
        // An eventual read's own failure is dropped, never recorded — it
        // carries no observation to check against anything.
        return;
    }

    let list = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|v| v.get("Item").map(decode_items_attr))
        .unwrap_or_default();
    if consistent {
        shared.rec.lock().expect("recorder poisoned").ok(
            proc,
            env.now().0,
            vec![Mop::Read {
                key,
                observed: Some(list),
            }],
        );
    } else {
        shared.record_eventual_read(key, list);
    }
}

/// Decode a `Query`/`Scan` response's `Items` array into one [`Mop::Read`]
/// per returned item, resolving each item's own `Key` from its `sk`
/// attribute via [`key_from_table_and_sk`] — see the module doc's
/// "Multi-key reads" section.
fn read_mops_from_items_response(table: u64, body: &str) -> Vec<Mop> {
    let Some(items) = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v.get("Items").and_then(Value::as_array).cloned())
    else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            let sk = item.get("sk")?.get("S")?.as_str()?;
            let key = key_from_table_and_sk(table, sk)?;
            Some(Mop::Read {
                key,
                observed: Some(decode_items_attr(item)),
            })
        })
        .collect()
}

/// A base-table `Query` scoped to one of [`PARTITIONS`] fixed partitions,
/// always `ConsistentRead: true` — proves `dispatch_item_op`'s base-table
/// `Query` arm returns exactly that partition's rows, feeding a
/// [`Mop::Read`] per returned item into the shared history as one
/// transaction.
async fn run_query(
    env: &SimEnv,
    handle: &SimClusterHandle,
    shared: &Arc<Shared>,
    proc: Process,
    table_idx: u64,
    table: &str,
    node: u64,
) {
    let part = env.gen_below(PARTITIONS);
    let pk = format!("part-{part}");
    shared
        .rec
        .lock()
        .expect("recorder poisoned")
        .invoke(proc, env.now().0, Vec::new());

    let body = json!({
        "TableName": table,
        "ConsistentRead": true,
        "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": {":p": {"S": pk}},
    })
    .to_string();
    let (status, body) = handle
        .dynamo(node, "DynamoDB_20120810.Query", body.as_bytes())
        .await;

    let mut rec = shared.rec.lock().expect("recorder poisoned");
    if status == 200 {
        rec.ok(
            proc,
            env.now().0,
            read_mops_from_items_response(table_idx, &body),
        );
    } else {
        rec.info(proc, env.now().0, Vec::new());
    }
}

/// A whole-table `Scan`, always `ConsistentRead: true` — [`run_query`]'s
/// unscoped sibling.
async fn run_scan(
    env: &SimEnv,
    handle: &SimClusterHandle,
    shared: &Arc<Shared>,
    proc: Process,
    table_idx: u64,
    table: &str,
    node: u64,
) {
    shared
        .rec
        .lock()
        .expect("recorder poisoned")
        .invoke(proc, env.now().0, Vec::new());

    let body = json!({"TableName": table, "ConsistentRead": true}).to_string();
    let (status, body) = handle
        .dynamo(node, "DynamoDB_20120810.Scan", body.as_bytes())
        .await;

    let mut rec = shared.rec.lock().expect("recorder poisoned");
    if status == 200 {
        rec.ok(
            proc,
            env.now().0,
            read_mops_from_items_response(table_idx, &body),
        );
    } else {
        rec.info(proc, env.now().0, Vec::new());
    }
}

/// One client's loop: each round, draw a fresh issuing node from the
/// scenario's own seed, then run one op — a write (single-writer, its own
/// owned keys only) or one of four read shapes across the whole keyspace.
#[allow(clippy::too_many_arguments)]
async fn client_loop(
    env: SimEnv,
    handle: SimClusterHandle,
    shared: Arc<Shared>,
    proc: Process,
    clients: usize,
    rounds: u64,
    tables: usize,
    keyspace: u64,
    read_pct: u64,
    node_count: usize,
) {
    let owned: Vec<Key> = (0..tables as u64)
        .flat_map(|t| (0..keyspace).map(move |k| t * TABLE_KEY_STRIDE + k))
        .filter(|&k| k % clients as u64 == proc)
        .collect();
    for _round in 0..rounds {
        let node = env.gen_below(node_count as u64);
        let is_read = env.gen_below(100) < read_pct;
        if is_read {
            let t = env.gen_below(tables as u64);
            let table = format!("t{t}");
            let kind = match env.gen_below(10) {
                0..=4 => ReadKind::ConsistentGet,
                5..=7 => ReadKind::EventualGet,
                8 => ReadKind::Query,
                _ => ReadKind::Scan,
            };
            match kind {
                ReadKind::ConsistentGet | ReadKind::EventualGet => {
                    let k = env.gen_below(keyspace);
                    let key = t * TABLE_KEY_STRIDE + k;
                    run_get(&env, &handle, &shared, proc, key, kind, node).await;
                }
                ReadKind::Query => run_query(&env, &handle, &shared, proc, t, &table, node).await,
                ReadKind::Scan => run_scan(&env, &handle, &shared, proc, t, &table, node).await,
            }
        } else if !owned.is_empty() {
            let key = owned[env.gen_below(owned.len() as u64) as usize];
            run_write(&env, &handle, &shared, proc, key, node).await;
        }
        env.sleep(POLL).await;
    }
}

/// `PutItem` → consistent `GetItem`(present) → `DeleteItem` → consistent
/// `GetItem`(absent), issued **from every node in the cluster in turn**, on
/// a `delete-probe-{node}` key namespace disjoint from the list-append
/// model's own `part-*`/`item-*` keys — the direct correctness check the
/// module doc's own "DeleteItem/BatchWriteItem" section explains. Mirrors
/// `sim_cluster_corpus.rs`'s own `run_delete_probe` (including its "drive
/// `SimCluster`'s own synchronous driver, never a bare `block_on` of
/// `SimClusterHandle`'s ops with nothing advancing the simulator" gotcha —
/// `cluster.dynamo` is that same synchronous driver, mirroring `put`/`get`/
/// `delete` exactly).
fn run_delete_probe(
    cluster: &mut SimCluster,
    table: &str,
    node_count: usize,
) -> Result<usize, String> {
    for node in 0..node_count as u64 {
        let pk = format!("delete-probe-{node}");
        let sk = "v";
        let put_body = json!({
            "TableName": table,
            "Item": {"pk": {"S": pk}, "sk": {"S": sk}, "items": one_element_list(9)},
        })
        .to_string();
        let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.PutItem", put_body.as_bytes());
        if status != 200 {
            return Err(format!(
                "node {node}: put failed: status={status} body={resp}"
            ));
        }

        let get_body = json!({
            "ConsistentRead": true,
            "TableName": table,
            "Key": {"pk": {"S": pk}, "sk": {"S": sk}},
        })
        .to_string();
        let (status, body) = cluster.dynamo(node, "DynamoDB_20120810.GetItem", get_body.as_bytes());
        if status != 200 || !body.contains(r#""N":"9""#) {
            return Err(format!(
                "node {node}: put not visible via a consistent get (status={status} body={body})"
            ));
        }

        let del_body = json!({
            "TableName": table,
            "Key": {"pk": {"S": pk}, "sk": {"S": sk}},
        })
        .to_string();
        let (status, resp) =
            cluster.dynamo(node, "DynamoDB_20120810.DeleteItem", del_body.as_bytes());
        if status != 200 {
            return Err(format!(
                "node {node}: delete failed: status={status} body={resp}"
            ));
        }

        let (status, body) = cluster.dynamo(node, "DynamoDB_20120810.GetItem", get_body.as_bytes());
        if status != 200 {
            return Err(format!(
                "node {node}: get after delete failed: status={status} body={body}"
            ));
        }
        if body.contains("\"Item\"") {
            return Err(format!(
                "node {node}: item still present after delete ({body})"
            ));
        }
    }
    Ok(node_count)
}

/// A two-item `BatchWriteItem`, verified with a consistent `GetItem` per
/// item, from every node in the cluster in turn — the module doc's own
/// direct correctness probe for `BatchWriteItem` (a whole-item overwrite,
/// kept out of the list-append model for the same reason `DeleteItem` is).
/// Own `batch-probe-{node}` key namespace, disjoint from every other key
/// this file's own probes/model use.
fn run_batch_write_probe(
    cluster: &mut SimCluster,
    table: &str,
    node_count: usize,
) -> Result<usize, String> {
    for node in 0..node_count as u64 {
        let pk = format!("batch-probe-{node}");
        let body = json!({
            "RequestItems": {
                table: [
                    {"PutRequest": {"Item": {"pk": {"S": pk}, "sk": {"S": "a"}, "items": one_element_list(1)}}},
                    {"PutRequest": {"Item": {"pk": {"S": pk}, "sk": {"S": "b"}, "items": one_element_list(2)}}},
                ],
            },
        })
        .to_string();
        let (status, resp) =
            cluster.dynamo(node, "DynamoDB_20120810.BatchWriteItem", body.as_bytes());
        if status != 200 {
            return Err(format!(
                "node {node}: BatchWriteItem failed: status={status} body={resp}"
            ));
        }

        for (sk, expect) in [("a", 1u64), ("b", 2u64)] {
            let get_body = json!({
                "ConsistentRead": true,
                "TableName": table,
                "Key": {"pk": {"S": pk}, "sk": {"S": sk}},
            })
            .to_string();
            let (status, body) =
                cluster.dynamo(node, "DynamoDB_20120810.GetItem", get_body.as_bytes());
            let seen = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|v| v.get("Item").map(decode_items_attr))
                .unwrap_or_default();
            if status != 200 || seen != vec![expect] {
                return Err(format!(
                    "node {node}: batch-written item {sk} not visible via a consistent \
                     get (status={status} body={body})"
                ));
            }
        }
    }
    Ok(node_count)
}

// ---------------------------------------------------------------------------
// The scenario runner.
// ---------------------------------------------------------------------------

struct ScenarioResult {
    cycles: CheckReport,
    durability: CheckReport,
    convergence: CheckReport,
    /// Every recorded `ConsistentRead: false` observation is a prefix of
    /// the converged final state — see the module doc's own
    /// "read-consistency modeling decision" section.
    eventual_prefix: CheckReport,
    ok_writes: usize,
    nonempty_reads: usize,
    non_hosting_ok_writes: usize,
    eventual_reads: usize,
    delete_probe: Result<usize, String>,
    batch_probe: Result<usize, String>,
}

fn combine_reports(seed: u64, reports: impl Iterator<Item = CheckReport>) -> CheckReport {
    let mut violations = Vec::new();
    for r in reports {
        violations.extend(r.violations);
    }
    CheckReport {
        ok: violations.is_empty(),
        violations,
        seed,
    }
}

/// Read every known key's raw stored value straight off `node`'s own local
/// engine (never routed) — the direct-decode sibling of
/// `sim_cluster_corpus.rs`'s own `final_state`, decoding this corpus's real
/// `animus_item`-encoded stored item instead of that file's own
/// hand-rolled `u64` list encoding.
fn final_state(
    handle: &SimClusterHandle,
    tablets: &BTreeMap<u64, TabletId>,
    node: u64,
    tables: usize,
    keyspace: u64,
) -> BTreeMap<Key, Vec<u64>> {
    let mut map = BTreeMap::new();
    for t in 0..tables as u64 {
        let Some(&tablet) = tablets.get(&t) else {
            continue;
        };
        for k in 0..keyspace {
            let key = t * TABLE_KEY_STRIDE + k;
            let (_, pk, sk) = table_pk_sk(key);
            let list = block_on(handle.local_value(node, tablet, &pk, &sk))
                .map(|b| decode_engine_items(&b))
                .unwrap_or_default();
            map.insert(key, list);
        }
    }
    map
}

fn check_eventual_reads_are_prefixes(
    seed: u64,
    observations: &[(Key, Vec<u64>)],
    final_state: &BTreeMap<Key, Vec<u64>>,
) -> CheckReport {
    let mut violations = Vec::new();
    for (key, observed) in observations {
        let converged = final_state.get(key).cloned().unwrap_or_default();
        if !converged.starts_with(observed) {
            violations.push(format!(
                "eventually-consistent read of key {key} observed {observed:?}, not a \
                 prefix of the converged final state {converged:?}"
            ));
        }
    }
    if violations.is_empty() {
        CheckReport {
            ok: true,
            violations,
            seed,
        }
    } else {
        CheckReport {
            ok: false,
            violations,
            seed,
        }
    }
}

fn run_scenario(s: &Scenario) -> ScenarioResult {
    let mut cluster = SimCluster::new(s.seed, s.nodes, s.replication);
    let table_names: Vec<String> = (0..s.tables).map(|t| format!("t{t}")).collect();
    let mut tablets: BTreeMap<u64, TabletId> = BTreeMap::new();
    for (i, name) in table_names.iter().enumerate() {
        let tablet = cluster.create_table_with_replication(name, s.replication);
        tablets.insert(i as u64, tablet);
    }
    let primary_tablet = tablets
        .get(&0)
        .cloned()
        .expect("table 0 must exist — every scenario creates at least one table");

    let shared = Arc::new(Shared {
        rec: Mutex::new(Recorder::new(s.seed)),
        next_value: Mutex::new(0),
        ok_writes_by_node: Mutex::new(BTreeMap::new()),
        eventual_reads: Mutex::new(Vec::new()),
    });

    let handle = cluster.handle();
    for c in 0..s.clients {
        let env = cluster.client_env(c as u64);
        let handle = handle.clone();
        let shared = Arc::clone(&shared);
        let (tables, rounds, keyspace, read_pct, node_count, clients) = (
            s.tables, s.rounds, s.keyspace, s.read_pct, s.nodes, s.clients,
        );
        let proc = c as Process;
        env.clone().spawn_task(async move {
            client_loop(
                env, handle, shared, proc, clients, rounds, tables, keyspace, read_pct, node_count,
            )
            .await;
        });
    }

    cluster.run_for(SETTLE);

    let mut elapsed = Duration::ZERO;
    for (at, nem) in s.faults.clone() {
        if at > elapsed {
            cluster.run_for(at - elapsed);
            elapsed = at;
        }
        nem.apply(&mut cluster, primary_tablet);
    }
    if !s.window.is_zero() {
        cluster.run_for(s.window);
    }
    cluster.heal_all();
    cluster.run_for(DRAIN);

    let history = shared
        .rec
        .lock()
        .expect("recorder poisoned")
        .history()
        .clone();
    let cycles = check_cycles(&history);

    let replicas = handle.replicas_of(primary_tablet);
    let all_states = |c: &SimCluster| -> Vec<BTreeMap<Key, Vec<u64>>> {
        let h = c.handle();
        replicas
            .iter()
            .map(|&n| final_state(&h, &tablets, n, s.tables, s.keyspace))
            .collect()
    };
    let mut states = all_states(&cluster);
    let mut durability = combine_reports(
        s.seed,
        states.iter().map(|st| check_durability(&history, st)),
    );
    let mut convergence = combine_reports(
        s.seed,
        states[1..]
            .iter()
            .map(|st| check_convergence(s.seed, &states[0], st)),
    );
    let poll_deadline_steps = CONVERGENCE_BUDGET.as_millis() / CONVERGENCE_POLL_STEP.as_millis();
    let mut polled: u128 = 0;
    while !(durability.ok && convergence.ok) && polled < poll_deadline_steps {
        cluster.run_for(CONVERGENCE_POLL_STEP);
        states = all_states(&cluster);
        durability = combine_reports(
            s.seed,
            states.iter().map(|st| check_durability(&history, st)),
        );
        convergence = combine_reports(
            s.seed,
            states[1..]
                .iter()
                .map(|st| check_convergence(s.seed, &states[0], st)),
        );
        polled += 1;
    }

    let eventual_observations = shared
        .eventual_reads
        .lock()
        .expect("eventual_reads poisoned")
        .clone();
    let eventual_prefix =
        check_eventual_reads_are_prefixes(s.seed, &eventual_observations, &states[0]);

    let ok_writes = history
        .ok_entries()
        .flat_map(|e| &e.mops)
        .filter(|m| matches!(m, Mop::Append { .. }))
        .count();
    let nonempty_reads = history
        .ok_entries()
        .filter(|e| {
            e.mops
                .iter()
                .any(|m| matches!(m, Mop::Read { observed: Some(l), .. } if !l.is_empty()))
        })
        .count();
    let ok_writes_by_node = shared
        .ok_writes_by_node
        .lock()
        .expect("ok_writes_by_node poisoned")
        .clone();
    let non_hosting_ok_writes: usize = (0..s.nodes as u64)
        .filter(|n| !replicas.contains(n))
        .map(|n| ok_writes_by_node.get(&n).copied().unwrap_or(0))
        .sum();

    let delete_probe = run_delete_probe(&mut cluster, &table_names[0], s.nodes);
    let batch_probe = run_batch_write_probe(&mut cluster, &table_names[0], s.nodes);

    ScenarioResult {
        cycles,
        durability,
        convergence,
        eventual_prefix,
        ok_writes,
        nonempty_reads,
        non_hosting_ok_writes,
        eventual_reads: eventual_observations.len(),
        delete_probe,
        batch_probe,
    }
}

fn run_scenario_identified(s: &Scenario) -> ScenarioResult {
    eprintln!("scenario={} seed={}", s.name, s.seed);
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_scenario(s))) {
        Ok(r) => r,
        Err(payload) => {
            let msg = payload
                .downcast_ref::<&str>()
                .map(|m| m.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "<non-string panic payload>".to_string());
            panic!("scenario={} seed={}: {msg}", s.name, s.seed);
        }
    }
}

fn scenario_failed(r: &ScenarioResult) -> bool {
    !r.cycles.ok
        || !r.durability.ok
        || !r.convergence.ok
        || !r.eventual_prefix.ok
        || r.delete_probe.is_err()
        || r.batch_probe.is_err()
}

fn assert_scenario_ok(s: &Scenario, r: &ScenarioResult) {
    assert!(
        r.cycles.ok,
        "scenario {} not serializable: {:?} (seed={})",
        s.name, r.cycles.violations, s.seed
    );
    assert!(
        r.durability.ok,
        "scenario {} lost an acked append: {:?} (seed={})",
        s.name, r.durability.violations, s.seed
    );
    assert!(
        r.convergence.ok,
        "scenario {} did not converge: {:?} (seed={})",
        s.name, r.convergence.violations, s.seed
    );
    assert!(
        r.eventual_prefix.ok,
        "scenario {} had a non-prefix eventually-consistent read: {:?} (seed={})",
        s.name, r.eventual_prefix.violations, s.seed
    );
    assert!(
        r.delete_probe.is_ok(),
        "scenario {} delete probe failed: {:?} (seed={})",
        s.name,
        r.delete_probe,
        s.seed
    );
    assert!(
        r.batch_probe.is_ok(),
        "scenario {} batch-write probe failed: {:?} (seed={})",
        s.name,
        r.batch_probe,
        s.seed
    );
}

// ---------------------------------------------------------------------------
// Failure minimization (ADR 0061 rung B4) — mirrors `sim_cluster_corpus.rs`'s
// own wiring exactly.
// ---------------------------------------------------------------------------

fn scenario_candidates(s: &Scenario) -> Vec<Scenario> {
    let mut out = Vec::new();
    if !s.faults.is_empty() {
        out.push(Scenario {
            faults: Vec::new(),
            ..s.clone()
        });
    }
    if !s.window.is_zero() {
        out.push(Scenario {
            window: Duration::ZERO,
            ..s.clone()
        });
    }
    if s.rounds > 1 {
        out.push(Scenario {
            rounds: (s.rounds / 2).max(1),
            ..s.clone()
        });
    }
    if s.keyspace > 1 {
        out.push(Scenario {
            keyspace: (s.keyspace / 2).max(1),
            ..s.clone()
        });
    }
    if s.clients > 1 {
        out.push(Scenario {
            clients: s.clients - 1,
            ..s.clone()
        });
    }
    out
}

fn shrink_and_report(s: &Scenario) -> ShrinkReport<Scenario> {
    let report = shrink::minimize(
        s.clone(),
        scenario_candidates,
        |cand| scenario_failed(&run_scenario(cand)),
        shrink::budget_from_env(),
    );
    eprintln!("{}", shrink::describe(&s.name, &report));
    match shrink::replay_json(&report) {
        Ok(json) => {
            eprintln!("  replay handle (JSON): {json}");
            eprintln!(
                "  Replay directly: ANIMUS_SHRINK_REPLAY='{json}' \\\n    \
                 cargo test -p animusd --lib sim_cluster_dynamo_shrink_replay \\\n    \
                 -- --ignored --nocapture"
            );
        }
        Err(e) => eprintln!("  (failed to serialize replay handle: {e})"),
    }
    report
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[test]
fn dynamo_wire_baseline_is_consistent() {
    let scenario = base_workload("dynamowire_baseline", 3, 3, 1, vec![], Duration::ZERO);
    let r = run_scenario(&scenario);
    assert_scenario_ok(&scenario, &r);
    assert!(r.ok_writes > 0, "no acked writes — vacuous run");
    assert!(
        r.nonempty_reads > 0,
        "no non-empty reads — checker had nothing to chew"
    );
}

#[test]
fn sim_cluster_dynamo_corpus_is_consistent() {
    let scenarios = corpus();
    let mut total_ok_writes = 0usize;
    let mut total_eventual_reads = 0usize;
    for s in &scenarios {
        let r = run_scenario_identified(s);
        if scenario_failed(&r) && shrink::shrink_enabled() {
            shrink_and_report(s);
        }
        assert_scenario_ok(s, &r);
        assert!(
            r.ok_writes > 0,
            "scenario {} did no acked writes — vacuous run (seed={})",
            s.name,
            s.seed
        );
        if s.nodes > s.replication {
            assert!(
                r.non_hosting_ok_writes > 0,
                "scenario {}: no write issued from a non-hosting node ever succeeded \
                 (seed={}) — forwarding may be broken, or the workload never actually \
                 exercised it",
                s.name,
                s.seed
            );
        }
        total_ok_writes += r.ok_writes;
        total_eventual_reads += r.eventual_reads;
    }
    assert!(
        total_ok_writes > scenarios.len(),
        "corpus too vacuous: only {total_ok_writes} acked writes across {} scenarios",
        scenarios.len()
    );
    assert!(
        total_eventual_reads > 0,
        "corpus never exercised a ConsistentRead:false read — the eventual-read \
         prefix check has nothing to prove"
    );
}

/// Coverage guard: every named fault class plus the fault-free forwarding
/// and multi-table cells must still be present at the frozen depth —
/// mirrors `sim_cluster_corpus.rs`'s own `sim_cluster_corpus_covers_every_
/// cell_shape`.
#[test]
fn dynamo_wire_corpus_covers_the_fault_matrix() {
    let cells = corpus_cells();
    assert_eq!(cells.len(), 8, "expected exactly 8 named cells");
    assert!(
        cells
            .iter()
            .any(|s| s.faults.is_empty() && s.tables == 1 && s.nodes == s.replication)
    );
    for nem in [
        Nemesis::LeaderCrash,
        Nemesis::FollowerCrash,
        Nemesis::StopRestart,
        Nemesis::LeaderPartition,
        Nemesis::SplitBrain,
    ] {
        assert!(
            cells
                .iter()
                .any(|s| s.faults.iter().any(|(_, f)| *f == nem)),
            "no cell schedules {nem:?}"
        );
    }
    assert!(
        cells.iter().any(|s| s.nodes > s.replication),
        "no forward-heavy (nodes > replication) cell"
    );
    assert!(cells.iter().any(|s| s.tables > 1), "no multi-table cell");
}

/// The replay entry point named in every `ANIMUS_SHRINK` report
/// (`shrink_and_report`'s printed instructions) — mirrors
/// `sim_cluster_corpus.rs`'s own `sim_cluster_shrink_replay`.
#[test]
#[ignore = "opt-in replay entry point — set ANIMUS_SHRINK_REPLAY to a shrink report's printed JSON"]
fn sim_cluster_dynamo_shrink_replay() {
    let Ok(json) = std::env::var("ANIMUS_SHRINK_REPLAY") else {
        eprintln!(
            "sim_cluster_dynamo_shrink_replay: skipped — set ANIMUS_SHRINK_REPLAY to a \
             shrink report's printed JSON to replay it"
        );
        return;
    };
    let scenario: Scenario =
        serde_json::from_str(&json).expect("ANIMUS_SHRINK_REPLAY must be a Scenario JSON blob");
    let r = run_scenario(&scenario);
    eprintln!(
        "replayed '{}' (seed={}): cycles.ok={} durability.ok={} convergence.ok={} \
         eventual_prefix.ok={} delete_probe={:?} batch_probe={:?} ok_writes={}",
        scenario.name,
        scenario.seed,
        r.cycles.ok,
        r.durability.ok,
        r.convergence.ok,
        r.eventual_prefix.ok,
        r.delete_probe,
        r.batch_probe,
        r.ok_writes
    );
    assert!(
        scenario_failed(&r),
        "replayed scenario '{}' (seed={}) did NOT reproduce the failure",
        scenario.name,
        scenario.seed
    );
}
