# ADR 0065 — Per-table throttling

- **Status:** Accepted
- **Date:** 2026-09-05
- **Origin:** `docs/roadmap.md`'s W-08 ("Per-table throttling")

## Context

`capacity.rs` (`animus-dynamo`) computes `ConsumedCapacity` per request — a
faithful reproduction of DynamoDB's published RCU/WCU arithmetic — but that
module's own doc says plainly why it stops at reporting: "AnimusDB has no
provisioned throughput and does not throttle... this is a **reporting**
surface." Nothing in this codebase gates a request today. A noisy tenant on
a shared cluster, or a single hot table, has no backpressure short of the
cluster's own hardware limits — no admission control at all, provisioned or
otherwise.

`docs/roadmap.md` §6 ("Deliberately not planned") is explicit that this ADR
is not building DynamoDB's billing story: "Global tables, on-demand/
provisioned billing, ... [are] properties of the managed service, declared
out of scope on `website/compatibility.html`. W-08 gives throttling without
a billing meter." This ADR designs admission control that speaks DynamoDB's
own capacity-unit vocabulary — so a real DynamoDB client's throttling
behavior (backoff on `ProvisionedThroughputExceededException`,
`UnprocessedItems`/`UnprocessedKeys`, `TransactionCanceledException`'s
`ThrottlingError` reason) works unmodified against this cluster — without
building the adjacent, explicitly out-of-scope metering/invoicing
machinery no client-visible behavior depends on.

W-09 (landed 2026-09-05, amending ADR 0034) built the per-tablet
`RequestRateTracker`/`RateSample` EWMA machinery this ADR's bucket sits
beside: a per-node, per-tablet map clocked on `env.now()`, GC'd against
`Metadata` via `retain_existing`, observed at exactly the two leader-side
write choke points (`dynamo::kind_write_item_at_leader`, the ADR 0046 U3
evaluate-at-leader funnel, and `dynamo::fast_marker_write`, the ADR 0049
fast arm) that together cover every non-transactional write. This ADR's
token bucket is a sibling structure at those same choke points, not a
reinvention: W-09 closed ADR 0034's deferred "no per-tablet request-rate
signal" bullet specifically so this ADR would not have to build one.

The design must also account for three facts already settled elsewhere in
this codebase, each of which shapes a decision below:

- **ADR 0054**: a write is *evaluated inside Raft apply*, in commit order —
  the leader only proposes an unevaluated operation (`KvCommand::KindEval`/
  `TxnStage`). Apply must stay deterministic, replayable from the log on
  every replica identically, and must never reject a *committed* entry —
  a token-bucket check performed inside apply would either need to be
  replicated state (defeating the point of a cheap local check) or would
  make apply itself nondeterministic across replicas whose local buckets
  disagree. Enforcement must therefore happen strictly *before* propose,
  never inside the state machine.
- **ADR 0055**: an eventually-consistent read (`ConsistentRead: false`, the
  wire default) is served from *any* replica's own applied state, with no
  read barrier and no leader involvement at all. A signal — request-rate,
  or a throttle bucket — that only the leader observes is structurally
  blind to this read path, exactly the reasoning W-09's own doc gives for
  excluding reads from `RequestRateTracker` entirely.
- **ADR 0018**: a multi-table transaction commits via 2PC across each
  write's own tablet leader (the anchor/participant stage protocol). A
  throttled participant has no ordinary single-item error path to return
  through — it must fail its own stage, which the existing abort machinery
  already turns into a whole-transaction cancellation.

## Decision

### 1. Scope and unit

Throttling is **per table**, expressed in DynamoDB capacity units exactly
as `capacity.rs` already computes them, and **enforced per tablet**: a
table's provisioned `ReadCapacityUnits`/`WriteCapacityUnits` are divided
evenly across its **current** tablet count, re-read from `Metadata` on
every check (never cached across a split) so a split re-divides the budget
automatically the moment the new tablet map commits — the identical
"re-derive from live `Metadata`, never a snapshot" discipline
`RequestRateTracker::retain_existing` and `auto_split_loop`'s own
threshold checks already follow.

**No cross-node coordination.** Each node keeps its own token bucket per
tablet it serves (leader or replica — see Decision 2). This mirrors
DynamoDB's own documented per-partition division of provisioned capacity,
and it is the only shape that keeps every enforcement point on the hot
path free of a round trip: a coordinated cluster-wide bucket would need a
consensus round (or at least a gossip round) per admission check, which is
a strictly worse cost than the write/read path it would be gating.

**Consequence, stated plainly**: because an eventually-consistent read can
be served by *any* of a tablet's RF replicas independently, and each
replica enforces its own local share of the read budget with no
cross-replica coordination, the aggregate eventual-read admission rate
across a tablet's replicas can reach up to **RF × the per-tablet read
share** before every replica's own bucket is exhausted. This is accepted,
not a bug to close: eventual reads already cost half an RCU per DynamoDB's
own formula, and the entire reason the cheap replica-local read path
exists (ADR 0055) is to avoid exactly the kind of cross-replica
coordination that would be needed to close this gap. A deployment that
cannot tolerate this multiplier should not rely on `ConsistentRead: false`
for a tightly-provisioned table — the same trade every DynamoDB user
already accepts when choosing eventual reads.

### 2. Enforcement points

**Writes** — checked on the tablet's **leader**, immediately before
proposing, at the two choke points every client write already funnels
through (mirroring W-09's own two-site coverage exactly, so the throttle
bucket and the rate tracker stay ticked from the identical call sites):

- `dynamo::kind_write_item_at_leader` (the ADR 0046 U3 evaluate-at-leader
  funnel: a condition, an old-image echo, or an images-carrying table).
- `dynamo::fast_marker_write` (the ADR 0049 fast arm: an unconditioned
  `Put`/`Delete` on a plain, unindexed/unstreamed table).

For a **transaction** (ADR 0018 §2), the identical write-side check runs
on each participant's own leader at `TxnPrepare`/stage time — the same
point `dynamo::eval_kind_txn_write`'s W-09 write-rate observation already
runs. A throttled participant fails its own stage; 2PC's existing abort
path turns that into a whole-transaction cancellation (see Decision 6 for
the wire shape).

**Reads** — checked on **whichever node serves the read**, after the
tablet is resolved (so the actual serving replica is known) and before the
engine work runs: the leader for a `ConsistentRead: true` read
(`cp_read`/`cp_scan`/`cp_scan_kind*`'s strong path), and **any** replica
for `ConsistentRead: false` (`cp_stale_local`/the ADR 0055 fast path) —
each replica enforcing its own local bucket independently, per Decision 1's
stated consequence.

A `Query`/`Scan` spanning several tablets charges **each tablet it actually
reads from** — the identical per-tablet accounting a multi-tablet fan-out
already uses for `ConsumedCapacity`'s own per-index/per-table totals,
extended to gate rather than only report.

**Never inside Raft apply.** A throttle decision is evaluated once, on the
proposing/serving node, strictly before the operation ever reaches the
log or the local engine read — never inside `KvCommand::KindEval`/
`TxnStage`'s apply arm, and never as a condition an entry's own commit can
fail on. This keeps apply exactly as deterministic and replayable as ADR
0054 already requires: a proposed entry, once accepted, applies
identically on every replica regardless of that replica's own bucket
state.

### 3. Cost model

Cost is **pre-charged from what is knowable before the operation runs**:

- **Writes**: the size of the request payload — the same `item_size`
  formula `capacity.rs`/`animus_item::size` already computes — rounded up
  to whole 1 KB WCU units (`write_units`, unchanged), doubled for a
  transactional write (`ConsumedCapacity::scaled(2.0)`'s existing factor).
  Index maintenance (GSI/LSI row writes) is charged following the existing
  `capacity.rs` rules — this ADR changes nothing about *what* is charged,
  only that the computed number is now also debited from a bucket.
- **Reads**: a read's cost cannot be known before it runs — DynamoDB has
  the identical constraint, since a `Query`/`Scan`'s result size is a
  function of what actually matched. The bucket is debited the **actual**
  bytes returned, after the read completes, rounded up to whole 4 KB RCU
  units and halved for an eventually-consistent read (`read_units`,
  unchanged) — allowed to drive the bucket **negative**, recovering on
  subsequent refill exactly as DynamoDB's own "a large item read can
  temporarily exceed a small partition's allotment" behavior already
  works. A negative bucket still returns the result that produced it (the
  read already happened); only the *next* read against that tablet
  observes the deficit.

### 4. Bucket shape

A token bucket **per tablet**, keyed identically to `RequestRateTracker`'s
own `BTreeMap<TabletId, _>`: refill rate = the tablet's current per-tablet
share (Decision 1, re-derived from live `Metadata` on every refill, not
fixed at bucket creation), capacity = **300 seconds** of that share — the
DynamoDB-documented burst window ("DynamoDB retains up to 300 seconds... of
unused capacity"). Clocked exclusively on `env.now()` (`Nanos`), never a
wall clock or `tokio::time::Instant` — both `write_path.rs` and
`read_path.rs` sit under `animusd`'s per-module
`#[deny(clippy::disallowed_methods)]` (ADR 0061 Phase C's closing rung), so
a `Instant::now()`/`SystemTime::now()` site here is a hard build failure,
not a review miss.

Stored beside `ChangeRateTracker`/`RequestRateTracker` in `lib.rs`
(`pub(crate) struct ThrottleTracker { inner: Arc<Mutex<BTreeMap<TabletId,
TokenBucket>>> }`, the identical `Arc<Mutex<..>>` shape those two already
use — every access is a quick lock/mutate/drop with no `.await` held
across it) and GC'd the same way: `retain_existing(&Metadata)` drops every
tablet no longer present in the live tablet map, called from the same
sweep site `ChangeRateTracker`/`RequestRateTracker` already are. Fully
deterministic under `SimEnv` — nothing about the bucket's refill/debit
arithmetic reads anything but `env.now()` and its own prior state.

### 5. Configuration, in two layers

**(a) Cluster-wide default** (S-06's `ClusterSettings`, `config.rs`): a new
`default_read_capacity_units: Option<u64>` /
`default_write_capacity_units: Option<u64>` pair (mirroring every other
`ClusterSettings` field's `Option<u64>` + `#[serde(default)]` shape and its
own `--default-*-capacity-units` CLI-flag/config-file "one way, not both"
contract), applied to any table that has not set its own provisioned
throughput. **Unset means `PAY_PER_REQUEST` semantics — no throttling —
which stays the default**, so an existing deployment's behavior is
byte-for-byte unchanged until an operator opts in at either layer.

**(b) Per-table** (the schema catalog, ADR 0013): `CreateTable`/
`UpdateTable` accept `BillingMode`
(`PROVISIONED`/`PAY_PER_REQUEST`) and, for `PROVISIONED`,
`ProvisionedThroughput { ReadCapacityUnits, WriteCapacityUnits }`,
replicated as a new `TableSchema` field —
`throughput: Option<ProvisionedThroughput>` — via a new `MetaCommand::
SetTableThroughput { table, spec: Option<ProvisionedThroughput> }`,
modelled directly on `MetaCommand::SetTableTtl`'s existing shape (`Some`
to set/change, `None` to fall back to `PAY_PER_REQUEST`/the cluster
default — a catalog no-op to re-set the identical spec, no disable-first
step required, the same reasoning `SetTableTtl`'s own doc gives for why
`TtlSpec` needs no separate identity to change in place). `DescribeTable`
reports `ProvisionedThroughputDescription`/`BillingModeSummary` from this
field, the same "pure read of the replicated catalog" shape
`describe_time_to_live` already is. **Per-table settings override the
cluster default** — a table with its own `throughput` set ignores
`ClusterSettings`' default entirely; a table with `throughput: None`
inherits the cluster default if one is configured, else is unthrottled.

`MetaCommand::SetTableThroughput` joins the `is_relayable_command`
allowlist beside `SetTableTtl`/`TagResource` — a table's provisioned
throughput must be settable from a follower-connected node exactly like
every other schema mutation (see the root `CLAUDE.md`'s "grep every gating
match site" standing rule).

### 6. Wire behaviour

- **Single-item operations** (`PutItem`/`GetItem`/`UpdateItem`/`DeleteItem`/
  `Query`/`Scan`) that hit an exhausted bucket return
  `ProvisionedThroughputExceededException` (HTTP 400), the AWS-faithful,
  client-SDK-retryable error every DynamoDB SDK already backs off on.
- **`BatchWriteItem`/`BatchGetItem`** never fail the whole call for a
  throttled item: a throttled write/read is returned in
  `UnprocessedItems`/`UnprocessedKeys` exactly as DynamoDB defines it,
  and every other item in the batch that didn't hit an exhausted bucket
  still commits/returns normally.
- **Transactions** (`TransactWriteItems`/`TransactGetItems`) cancel with
  `TransactionCanceledException` carrying a `ThrottlingError` cancellation
  reason at the throttled action's own index — the same
  `CancellationReasons` array shape every other 2PC abort reason
  (`ConditionalCheckFailed`, `None`) already populates, per ADR 0018 §2's
  existing abort-reason plumbing.
- **`ConsumedCapacity` reporting is unchanged and independent of
  throttling** — a throttled request never gets far enough to have
  consumed anything, and a request that succeeds reports exactly what
  `capacity.rs` always computed; this ADR adds a gate, not a second
  accounting system.

### 7. Observability

Per-tablet throttle counters through the ADR 0015 metrics seam:
`Metric::ThrottledReads`/`Metric::ThrottledWrites`, incremented at each
enforcement point in Decision 2. Surfaced on `/admin/metrics` as a new
`throttle` array (one entry per currently-tracked tablet: tablet id,
current bucket level, configured capacity, throttled-read/write counts),
beside the existing `request_rates` array — the identical
`ThrottleTracker::snapshot()` → `ClientCtx::throttle_state`
→ `admin::metrics_view` plumbing shape `RequestRateTracker::snapshot()`
already has.

### 8. Out of scope, stated

- **Adaptive capacity / hot-partition rebalancing of the per-tablet
  share.** DynamoDB's adaptive capacity dynamically shifts unused
  partition capacity toward a hot partition; this ADR's even division is
  static and re-derived only on a tablet-count change (a split), never on
  observed skew. A hot tablet under a table-wide budget divided evenly
  may throttle before the table's *aggregate* provisioned capacity is
  exhausted — a real, accepted limitation, not an oversight.
- **Auto-scaling** (DynamoDB's `ApplicationAutoScaling`-driven provisioned
  capacity adjustment) — a table's `ProvisionedThroughput` is a value an
  operator sets via `UpdateTable`, never one this codebase adjusts on its
  own.
- **Billing/metering.** Named explicitly out of scope by `docs/
  roadmap.md` §6 — nothing here produces an invoice-shaped record; the
  token bucket exists purely to gate admission.
- **Per-GSI provisioned throughput.** A GSI shares its base table's
  budget — its write-amplification cost is already charged against the
  base table's WCU bucket via the existing `capacity.rs` index-maintenance
  rules (Decision 3); there is no independent GSI-level `ProvisionedThroughput`
  or bucket, matching this codebase's existing "a GSI's cost rides the base
  table's accounting" posture.
- **Any cross-node token exchange** — see Decision 1's "no cross-node
  coordination" and its accepted RF-multiplier consequence for eventual
  reads.

## Consequences

- A table can be genuinely throttled for the first time — a real
  admission-control mechanism where none existed, closing the gap
  `capacity.rs`'s own module doc names.
- The mechanism composes cleanly with every existing design decision it
  touches: it adds no new state to Raft apply (ADR 0054), respects the
  eventually-consistent read path's replica-local, no-leader-involvement
  contract (ADR 0055) at the cost of an accepted RF burst multiplier, and
  reuses 2PC's existing abort/cancellation-reason machinery for a
  throttled transactional participant (ADR 0018) rather than inventing a
  new failure mode there.
- **Existing deployments are unaffected by default** — `PAY_PER_REQUEST`
  (no `ClusterSettings` default, no per-table `ProvisionedThroughput`) is
  the default at both configuration layers, so a cluster that never opts
  in never throttles, byte-for-byte unchanged from before this ADR.
- **A hot tablet within an evenly-divided table budget can throttle before
  the table's aggregate provisioned capacity is exhausted** (Decision 8) —
  an accepted, documented limitation an operator can only work around
  today by provisioning generously or splitting the table more finely
  (which re-divides the budget across more tablets, Decision 1), not by
  any adaptive mechanism this ADR builds.
- **Eventually-consistent reads admit up to RF× the nominal per-tablet
  read share in aggregate** (Decision 1) — accepted, not a defect, for
  the reasons stated there.
- A new replicated `TableSchema` field (`throughput`) and a new
  `MetaCommand` variant join the schema catalog's existing surface,
  following the identical pattern `TtlSpec`/`SetTableTtl` already
  established — no new catalog *mechanism*, just one more optional,
  independently-settable spec.

## Testing

- A `SimEnv` virtual-clock test proving the token bucket's own refill/debit/
  burst arithmetic deterministically (no real sleep — `Simulator::run_for`
  advances the clock the same way W-09's own `rate_tracker_tests` module
  does), and, where practical, exercised over the D1 `SimCluster` fixture
  (`sim_cluster.rs`) so a throttled write's leader-side rejection and a
  throttled eventual read's replica-local rejection are both proven through
  real route/forward paths, not just the bucket primitive in isolation.
- A real-thread `animusd` integration test, `tests/dynamo_throttling.rs`,
  covering the wire shapes end to end: a single-item `PutItem`/`GetItem`
  returning `ProvisionedThroughputExceededException` once a table's
  configured budget is exhausted; a `BatchWriteItem`/`BatchGetItem` call
  returning the throttled subset in `UnprocessedItems`/`UnprocessedKeys`
  while the rest succeeds; a `TransactWriteItems` call cancelling with
  `TransactionCanceledException` and a `ThrottlingError` reason at the
  correct index; a split re-dividing a table's budget across its new
  tablet count (an auto-split or `POST /admin/tablet/split` mid-test);
  and the cluster-default-vs-per-table-override precedent from Decision 5.
- `/admin/metrics`'s new `throttle` array asserted directly, mirroring
  `tests/admin_endpoint.rs`'s existing `request_rates` assertions.
- `MetaCommand::SetTableThroughput`'s follower-relay regression lives
  beside `SetTableTtl`/`TagResource`'s own in `tests/schema_ddl_relay.rs`,
  the established precedent for a new DDL command joining
  `is_relayable_command`.

## Alternatives considered

- **A single coordinated cluster-wide bucket per table** (a control-plane-
  replicated counter, or a gossip-exchanged token pool). Rejected: every
  enforcement point in Decision 2 sits directly on the write and read hot
  paths; a coordinated bucket would need a consensus or network round trip
  per admission check — strictly more expensive than the operation it
  gates, and a direct contradiction of ADR 0055's whole reason for the
  cheap, no-leader-involvement eventual-read path existing at all. The
  per-tablet, per-node local bucket (Decision 1) is the only shape that
  keeps every check purely local.
- **Enforcing inside Raft apply** (a token check as part of `KvCommand::
  KindEval`/`TxnStage`'s apply arm). Rejected: ADR 0054 requires apply to
  be deterministic and replayable identically on every replica from the
  committed log; a replica-local bucket read inside apply would make two
  replicas' applied state diverge depending on each one's own bucket
  history, and a committed entry must never be rejectable by apply at all
  (a Raft-committed entry is, by definition, going to be applied
  everywhere — refusing it on some replicas and not others is a
  correctness bug, not a throttling decision). Enforcement belongs
  strictly before propose, on the node that decides whether to propose in
  the first place.
- **Per-node, table-level buckets with no per-tablet division.** Rejected:
  a table's tablets are typically spread across several nodes (ADR 0005's
  placement policy), so a per-node table-level bucket would need either
  cross-node coordination to enforce one coherent table-wide budget (the
  first rejected alternative's cost, again) or would silently let a
  table's *effective* aggregate throughput scale with however many nodes
  happen to host its tablets — an unpredictable, deployment-shape-
  dependent limit rather than the operator-configured one DynamoDB
  clients expect. Dividing evenly across the table's own current tablet
  count (Decision 1) is deployment-shape-independent by construction and
  mirrors DynamoDB's own documented per-partition division.

## 2026-09-05 amendment — W-08 step 4, as built

All four steps landed as designed, with one naming deviation from Decision
5(a)'s literal text: the cluster-wide default fields/flags are
`ClusterSettings::{throttle_read_units, throttle_write_units}` /
`--throttle-read-units`/`--throttle-write-units`, not
`default_{read,write}_capacity_units` / `--default-*-capacity-units` as
originally written — shorter, and consistent with this same field pair's
name everywhere else in the implementation (`AdminInfo`, `ClientCtx::
throttle_defaults`, `/admin/config`'s JSON keys, `ThrottleDefaults::new`).
No behavioral difference from the ADR's own text: unset still means
`PAY_PER_REQUEST` at both layers, a per-table `TableSchema.throughput`
still overrides the cluster default entirely (no per-field merge), and the
CLI-flag-and-config-file-both-set contract is the identical "one way, not
both" hard error every other `ClusterSettings` field already has.

Everything else matches Decision 5 as written: `TableSchema.throughput:
Option<ProvisionedThroughput>`, `MetaCommand::SetTableThroughput{table,
spec}` modeled on `SetTableTtl`, on the `is_relayable_command` allowlist;
`CreateTable`/`UpdateTable` accept `BillingMode`/`ProvisionedThroughput`;
`DescribeTable` reports `ProvisionedThroughputDescription`/
`BillingModeSummary`. One decode-shape decision the ADR text didn't spell
out: `UpdateTable`'s throughput change joins `GlobalSecondaryIndexUpdates`/
`StreamSpecification` as a third, mutually-exclusive per-call change (Fork
C, extended) — except a bare `BillingMode: "PAY_PER_REQUEST"` restatement
alongside a real stream/index change, which stays tolerated as a no-op
(the pre-ADR-0065 precedent for that exact shape, a common SDK/CLI habit).
