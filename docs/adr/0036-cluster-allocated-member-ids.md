# ADR 0036 — Cluster-allocated member ids

- **Status:** Superseded by [ADR 0040](0040-self-minted-string-node-ids.md)
  (2026-08-11). Amended by ADR 0037's hardening trio PR 3, which wired this
  allocator into `control-add` — see this doc's "Follow-on work" section
  below for that pointer. **The whole mechanism this ADR describes is now
  gone**: `MetaCommand::AllocateNodeId`, `Metadata.next_alloc_id`/
  `node_id_allocations`, `ALLOC_ID_BASE`, `syskv::EntityKind::NodeIdAlloc`,
  `config::synthetic_control_id_for`, and `generate_join_nonce`'s
  OS-randomness exception are all deleted, replaced by self-minted string
  `NodeId`s (`NodeId::mint`) plus a replicated registration
  compare-and-swap (`MetaCommand::RegisterNode`) — see ADR 0040's Decisions
  B and C. This doc is kept for historical context (the CAS-allocation
  reasoning it argues for is exactly what ADR 0040 generalizes); do not
  implement anything new against the design below.
- **Date:** 2026-08-10

## Context

`animusd join --seed ADDR[,ADDR...] --node I` (ADR 0032 PR2) and `animusd
data --seed ADDR[,ADDR...] --node I` (ADR 0035 PR5) both require the operator
to pick `I` by hand. The joining process derives its ids from it
(`control_id(I) = I`, `raftkv_id(I) = 300 + I`, in
`crates/animusd/src/config.rs`) and the only collision protection is a
pre-bind, best-effort check: `run_node_join`/`run_node_data_join`'s
`check_join_collision` fetches a `ClientRequest::Status` reply and compares
`Metadata.node_addrs` at that id for exact address-book equality. ADR 0032's
own PR2 decision section says this plainly: an **identical** entry is a
rejoin and proceeds; a **different** one fails loudly with `AlreadyExists`;
but "this narrows, but does not fully eliminate, the race between two
simultaneous joiners choosing the same index — `RegisterNodeAddrs`'s own
idempotent apply-time check is the actual backstop for that residual window,
not this pre-bind check." Two operators (or two scripts) picking the same
`--node I` at the same moment can still both pass the pre-bind check before
either has registered anything, and only `RegisterNodeAddrs`'s
identical-input idempotence saves them — which only works if their address
books happen to collide byte-for-byte; if the two joiners bind to different
addresses, one join silently loses.

A related but distinct problem: automating cluster growth (e.g. from an
orchestrator that spins up N nodes without coordinating index assignment) is
harder than it needs to be with the `--node I` contract, since the caller has
to either serialize joins or track a shared counter itself. The cluster
already has a *server-side, CAS-style* atomic allocator for exactly this
shape of problem — the tablet-id allocator (`Metadata::next_tablet_id`/
`next_free_tablet_id`, ADR 0023): a monotonic counter, gated at apply time,
that makes two racing proposers land on two distinct ids with **no**
pre-check needed, because the state machine itself enforces uniqueness.

**Framing correction, called out explicitly here because it is easy to
misread ADR 0032's own words:** that ADR's PR2 decision section says `--node
I` was chosen "rather than inventing a new auto-numbering scheme this PR
doesn't need." This ADR is not that rejected scheme. An "auto-numbering
scheme" in the sense ADR 0032 meant it is a **client-side guess** (e.g.
"pick the next free small integer") — exactly the kind of thing that
reintroduces a race the moment two clients guess concurrently. What this ADR
adds is **server-side, CAS-style, atomic allocation**: the id is minted by
the replicated state machine itself, under the same total-order guarantee
that already makes two racing `CreateTablet`s land on two distinct tablet
ids. That is a materially stronger mechanism than "a scheme for guessing
numbers," which is why it gets its own ADR rather than amending 0032's
decision in place.

## Decision

### A new replicated command: `MetaCommand::AllocateNodeId`

`crates/animus-control/src/meta.rs` gains:

- `pub const ALLOC_ID_BASE: NodeId = 1_000_000;` — comfortably above
  `RAFTKV_ID_BASE` (300, `animusd`) plus any realistic manually-configured
  node count. `animus-control` has no dependency on `animusd`, so this is
  documented in prose rather than shared as a constant — the same discipline
  already used for `NodeAddrs.role`'s plain-`String` vocabulary.
- `Metadata.next_alloc_id: NodeId` (`#[serde(default = "..."`, defaulting to
  `ALLOC_ID_BASE`) and `Metadata.node_id_allocations: BTreeMap<String,
  NodeId>` (`#[serde(default)]`) — the monotonic counter and the
  **idempotency ledger** (nonce → allocated id), mirroring
  `next_tablet_id`/`merged_tablets` respectively.
- `Metadata::next_free_alloc_id(&self) -> NodeId` — the `ALLOC_ID_BASE`-range
  dual of `next_free_tablet_id`: folds the counter together with the highest
  id already seen in `members` or `node_id_allocations`, floored at
  `ALLOC_ID_BASE`.
- `MetaCommand::AllocateNodeId { nonce: String, labels: BTreeMap<String,
  String> }`, applied as:
  ```rust
  MetaCommand::AllocateNodeId { nonce, labels } => {
      if self.node_id_allocations.contains_key(nonce) {
          return ApplyOutcome::NoOp; // idempotent replay
      }
      let node_id = self.next_free_alloc_id();
      self.next_alloc_id = node_id + 1;
      self.node_id_allocations.insert(nonce.clone(), node_id);
      self.members.insert(node_id, Member { labels: labels.clone(), status: NodeStatus::Down });
      ApplyOutcome::Applied
  }
  ```
  No epoch-CAS is needed — uniqueness comes from the monotonic floor plus a
  presence check, the same shape `SplitTablet`'s allocator guard already
  uses for tablet ids. `nonce` is a **joiner-generated idempotency key**:
  replaying the same nonce (a proposer retry after an `Accepted`-but-
  unconfirmed propose — the durable-before-visible discipline every
  proposer in this codebase must respect) is a no-op that returns the
  identical, already-allocated id, so a retried join attempt can never mint
  a second id for itself.

The new member lands `Down`, with no address — the address arrives later via
the joiner's own, completely unchanged `RegisterNodeAddrs` self-registration
(ADR 0032 PR1). The detector still requires a real heartbeat before the
member becomes `Active` and placement-eligible (ADR 0012, unchanged).

### Wire/CLI (`animusd`)

- `ClientRequest::AllocateNodeId { nonce, labels }` /
  `ClientResponse::NodeIdAllocated { node }`: any node answers this fully —
  `ClientCtx::allocate_node_id` is structurally identical to
  `trigger_split`, proposing `MetaCommand::AllocateNodeId` (locally if
  leader, else relayed) and polling `effective_metadata().node_id_allocations`
  for the nonce's entry until it commits or `SCHEMA_COMMIT_TIMEOUT` elapses.
  The whole propose-then-confirm loop runs server-side in one round trip, so
  a joining process — which has no local `Metadata` yet to poll itself —
  just waits for one reply.
- **`is_relayable_command` gains `MetaCommand::AllocateNodeId { .. }`** — the
  single highest-risk line in this change (a missed allowlist entry is a
  bimodal per-process flake the compiler cannot catch, per the root
  `CLAUDE.md`'s standing warning): a joining process has no local control
  role at all yet, so this request is its *only* way to reach the real
  leader when it happens to contact a follower-connected seed. Safe for the
  same reason `UpsertMember{Down}` (ADR 0030) already is: `AllocateNodeId`'s
  apply always registers the new member `Down`, granting no placement
  eligibility by itself.
- `config::synthetic_control_id_for(raftkv_id) -> NodeId` (`raftkv_id | (1 <<
  63)`): a **local, non-replicated** placeholder control id for a
  combined-mode allocated join's permanent-non-voter control `RaftCore` —
  the exact structural mechanism ADR 0030 §3 already established for a
  `--node`-indexed growth/join node (a control id outside `control_ids` is a
  safe permanent non-voter). An allocated join has no small operator index
  to derive a control id from, so it derives one from the freshly allocated
  raftkv id instead. Never written to replicated `Metadata`, never dialed by
  another process — purely local, like `control_id(index)` itself. A
  data-only allocated join doesn't need this (no local control role at all).
- New entry points `run_node_join_allocated`/`run_node_data_join_allocated`,
  **additive** — `run_node_join`/`run_node_data_join` are unchanged, byte for
  byte. Discovery (`discover_join_info`) is reused verbatim; the allocated
  path **skips `check_join_collision` entirely** (there is nothing to
  collide with an id minted fresh by construction), asks the cluster for an
  id via the new `allocate_node_id` helper, then finishes through the same
  `finish_combined_join`/`finish_data_join` tail the operator-indexed path
  uses (factored out of `run_node_join`/`run_node_data_join` in this change,
  with no behavior change to either).
- `--node I` becomes **optional** on `join` and `data --seed`
  (`crates/animusd/src/main.rs`): present, byte-for-byte the existing path;
  absent, an explicit `--base-port` is now **required** (a hard error, no
  silent default) — an allocated id is not a small index, so there is no
  conventional index-derived port range to fall back to, and the caller must
  pick a `--base-port` whose six-port block doesn't collide with anyone
  else's.

### The nonce, and the one deliberate `Env`-seam exception

`generate_join_nonce` (`crates/animusd/src/lib.rs`) draws real (non-`Env`)
randomness — a **deliberate, narrowly commented exception** to the `Env`-seam
rule (ADR 0003: no unseeded randomness outside `ProdEnv`/test code). It is
called exactly once, at the CLI pre-bind boundary of
`run_node_join_allocated`/`run_node_data_join_allocated`, before any listener
is bound and before anything `SimEnv` could ever drive — these are
real-process, real-`TcpStream` entry points with no sim-test caller (the
control-plane sim coverage of `AllocateNodeId` itself, in
`animus-control`'s `tests/node_id_allocation.rs`, drives the `MetaCommand`
directly and never touches this wrapper). Analogous to the OS handing out an
ephemeral TCP port: the nonce only needs to be practically unique for the
lifetime of one join attempt, not cryptographically strong or globally
unique forever.

## Back-compat / operator semantics

`--node I` remains the **durable-identity** path (unchanged rejoin
semantics, ADR 0032 PR2/PR3): a restart at the same index/addresses/dir is
recognized and proceeds normally.

Omitting `--node` is an **ephemeral-identity** join: a restart — the same
process retried after losing its own in-memory nonce, or a fresh process
started with a fresh dir — allocates a **new** id via a fresh nonce. The old
id's `Member` entry lingers `Down`/address-less forever (ids are never
reused, mirroring the tablet-id allocator's own non-reuse invariant) until
pruned via the existing decommission path — `RemoveMember` already permits
removing a `Down`, unreferenced member, so no new cleanup mechanism was
needed. An operator who wants durable identity across restarts should keep
using `--node I`.

## Consequences

- Closes ADR 0032's own documented residual race (two simultaneous
  `--node`-indexed joiners choosing the same index) for anyone who opts into
  omitting `--node` — by construction, not by narrowing a window: the
  allocator's disjoint range plus its apply-time presence check make two
  racing allocations always land on two distinct ids, on every replica,
  with no epoch-CAS and no pre-bind guess required.
- **Orphaned `Down` allocations** from abandoned join attempts (the process
  crashes before ever self-registering an address) are accepted as a
  consequence, not a leak to fix later: bounded, and cleanable through the
  existing `RemoveMember` path exactly like any other drained, unreferenced
  member once an operator notices it.
- **`ALLOC_ID_BASE` collision with a real cluster manually configured past
  ~1M nodes** is not enforced at apply time — an explicit non-goal,
  consistent with this codebase's existing non-enforcement of other
  disjoint-range assumptions (e.g. `RAFTKV_ID_BASE` itself).
- `synthetic_control_id_for`'s top-bit trick depends on `NodeId` staying a
  `u64` and no real id space (control ids, `RAFTKV_ID_BASE`-offset raftkv
  ids, `ALLOC_ID_BASE`-offset allocated ids) ever approaching `2^63` — noted
  in that function's own doc, not enforced.
- **Two genuinely separate CLI contracts** for `join`/`data --seed`
  (`--node` present vs. absent) rather than a shared default — verified by
  keeping `run_node_join`/`run_node_data_join` completely untouched and
  adding new, additive entry points for the allocated path.
- Follow-on work this sets up, not yet built: control-plane membership
  change (a future ADR) would let an allocated id's node become a *real*
  control voter instead of a permanent non-voter; a system keyspace (a
  future ADR) could expose `node_id_allocations`/`next_alloc_id` as queryable
  rows instead of only through `/admin/status`'s raw `Metadata` dump.

  **Done: control-plane membership change (ADR 0037) landed and its own
  hardening trio's PR 3 wires this allocator into `POST
  /admin/control/member/add`** — `node` is optional; omitted, it mints from
  this same allocator instead of requiring an operator-chosen id (with the
  member-collision and `ALLOC_ID_BASE` refusals skipped only for the id it
  just minted). See ADR 0037's "Coordination with ADR 0036" section for the
  mechanism. The system-keyspace half of this bullet remains open.

## Superseded by ADR 0040

[ADR 0040](0040-self-minted-string-node-ids.md) replaces this ADR's whole
mechanism outright, in its own PR4: the monotonic `ALLOC_ID_BASE` counter +
`node_id_allocations` idempotency ledger become a **self-minted, validated
string** `NodeId` (`NodeId::mint`, two `Rng` draws base64url-encoded) plus a
**replicated registration compare-and-swap** (`MetaCommand::RegisterNode`)
instead of a monotonic-counter-plus-presence-check apply. The reasoning this
ADR's Context section makes for CAS-style server-side allocation over a
client-side guess carries over unchanged — ADR 0040 generalizes it rather
than replacing the *argument*, only the mechanism: uniqueness no longer
depends on ids being small dense integers from one counter, so the same
CAS-style guarantee now works for an id space where any client can also
*propose* its own durable name (this ADR's design had no room for that —
every id was allocator-derived by construction).

Also retired by ADR 0040 PR1 (which landed *before* PR4, since Option B did
not require the string representation): `config::synthetic_control_id_for`'s
top-bit placeholder-control-id trick this ADR's Decision section introduces.
ADR 0040 unifies a node's control-Raft and data-plane identities into one id
(Decision A) up front, so an allocated/minted join's id simply *is* its one
identity — there is no second, control-side id space left to need a derived
placeholder for.

`generate_join_nonce`'s one deliberate `Env`-seam exception (this ADR's own
"one deliberate `Env`-seam exception" section) is replaced by
`animus_env::prod::PreBindRng` — a reusable, documented pre-bind entropy
seam rather than a one-off function scoped to this allocator's single call
site.
