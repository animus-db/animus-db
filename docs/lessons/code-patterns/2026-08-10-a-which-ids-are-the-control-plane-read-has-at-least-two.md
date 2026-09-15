# A "which ids are the control plane" read has (at least) two structurally different purposes — a *seed* for a node's own bring-up vs. a *live authority* for a running correctness decision — and only an explicit, named audit catches every instance of the second kind hiding behind the first kind's static source.

**A "which ids are the control plane" read has (at least) two structurally
different purposes — a *seed* for a node's own bring-up vs. a *live
authority* for a running correctness decision — and only an explicit,
named audit catches every instance of the second kind hiding behind the
first kind's static source.** Auditing every `control_ids`/`admin.
control_ids`/`ClusterConfig::control_ids()` read in `animusd` for ADR 0037
PR4 (the plan's own named deliverable, mirroring the ADR 0029 ReadIndex-
quorum lesson's warning that this exact class of bug is invisible until a
*real* membership change exercises the divergence): the overwhelming
majority are legitimately static — `RaftNode::start`'s initial `all_nodes`
at process bring-up, `ClusterConfig::control_ids()`'s config-file-derived
helper (there is no "live" analogue for a plain config accessor), and
`ClientRequest::JoinInfo`'s reply (a joining node's *seed*, which the
replicated `node_addrs` overlay + this same PR's `control_peer_sync_loop`
already keep current after that point, same as every other ADR 0032 PR1
seed-then-overlay axis). Exactly **one** site was a live-authority
decision wearing a static-seed's clothes: `admin_remove_member`'s
control-voter refusal, fixed to read `self.control.config()` (see the
`docs/engineering-lessons.md` entry on `ControlHandle::config()`'s
`Option`, and `animusd/CLAUDE.md`'s decommission gotcha). **One further
site was flagged, not fixed, as an accepted, narrower, deliberately
out-of-scope gap**: `heartbeat_loop`'s `control_ids` parameter (a raftkv-
role node's heartbeat *destination* list, captured once at that node's own
process start) never gets a live-overlay refresh the way `peer_sync_loop`/
`route_sync_loop`/this PR's own `control_peer_sync_loop` do for their
respective axes — so a raftkv node started before a control voter was
added at runtime never heartbeats that voter directly, and if it later
becomes leader, this specific already-running raftkv node's heartbeats
keep missing it (a *bounded*, self-healing gap in practice: the *other*
raftkv nodes docker/replicas/heartbeats still reach it, and a restart of
the affected node picks up the current `control_ids` again) rather than a
silent total loss of failure detection. Fixing it properly is the same
"port `peer_sync_loop`'s pattern to a new axis" shape this PR already did
twice (`control_peer_sync_loop` for the control role's own peer book,
PR2's `node_addrs`/`Status.control_voters` wiring for discovery) — sizing
it as its own follow-up rather than a third instance crammed into this PR
keeps the diff reviewable, per this file's own standing don't-conflate-
unrelated-fixes discipline. **General rule for any future "is this id
part of X" read**: ask whether getting it wrong for one tick is (a)
"a joining node briefly doesn't know about a very recent peer, self-heals
next sync tick" (fine, static) or (b) "a decision that, once made, is hard
or impossible to undo, or degrades a safety/liveness property with no
self-healing path" (must read the live authority) — and write the answer
down at the call site, not just in an audit PR's description, so the next
reader doesn't have to re-derive it.

**Update: the flagged `heartbeat_loop` gap above is now closed (PR #134,
the ADR 0037 hardening trio's PR 1)** — and closing it surfaced a second,
previously-undocumented gap the original text above never named at all,
which is itself the generalizable lesson: **a "this destination list is
static" finding must also check the transport *address book* — the two
are separate staleness axes that fail together, and fixing only the list
leaves the send silently dropped anyway.** `heartbeat_loop`'s
`control_ids` argument was one axis (*which ids* to heartbeat); the
raftkv env's own peer book was the other (*where* to actually reach each
of those ids) — `peer_sync_loop` already refreshed the book from
`Metadata.cp_member_addrs`/`node_addrs[*].raftkv` on a timer, but never
from `node_addrs[*].control`, so a runtime-added control voter's address
never landed there. A destination list naming a live id with no matching
address book entry is not a partial fix — `ProdEnv::send`'s fire-and-forget
contract means the gap stays *exactly* as silent as the one being fixed
(no error, no log at above debug, just a heartbeat that never arrives).
The fix, once both axes were identified, was mechanically the same shape
each time this class of bug has appeared in this ADR's own stack
(`control_peer_sync_loop`'s `.control` merge for the control role's own
peer book, PR2's `node_addrs`/`Status.control_voters` wiring for
discovery): (a) a new animusd-local `heartbeat_loop_live` re-derives the
destination list every tick from `ctx.control.config()` instead of the
bring-up-time snapshot `animus_control::node::heartbeat_loop` was pinned
to (that function itself, and its `SimEnv` call sites, are deliberately
untouched — the static-list contract is still correct there); (b)
`peer_sync_loop` gained the missing `node_addrs[*].control` merge,
alongside its existing `.raftkv`/`cp_member_addrs` ones. **General rule
for any future "is this destination list live" audit**: grep the sending
env's own peer-book refresh loop in the same pass — a live list and a
stale book produce the identical externally-visible symptom (silence),
so a test that only asserts on the destination-list computation, without
driving a real send through a real socket, will not catch the second
axis. `tests/heartbeat_live_destinations.rs::
heartbeat_reaches_a_runtime_added_voter_after_it_becomes_leader` catches
both together: it grows a control voter at runtime, forces a
deterministic 2-voter leadership transfer onto it, and polls the new
leader's own `/admin/raft` view for a *sustained* `believes_alive: true`
across several `DETECT_TIMEOUT` windows — a test that fixed only the
list (or only the book) would still fail this, since the other half's
silent drop is indistinguishable from "no fix at all" to anything short
of a real end-to-end delivery check.
