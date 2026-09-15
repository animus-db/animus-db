# The rebalancer converges the *global* imbalance to `max − min ≤ 1` and stops — it makes no per-table promise, so a test must not route an op through ONLY a just-grown node for an *arbitrary* table.

**The rebalancer converges the *global* imbalance to `max − min ≤ 1` and
stops — it makes no per-table promise, so a test must not route an op
through ONLY a just-grown node for an *arbitrary* table.** Building the ADR
0032 PR2 seed/join test, "the joined node hosts a replica of *some* tablet"
is the stable rebalancing signal, but writing through only that node's
client address for a table it does *not* replicate flakes bimodally
(~40%): `resolve_cp_route`'s no-local-replica branch forwards blindly to
*some known replica* of the tablet — not its leader — and the receiving
`cp_serve_forwarded` never re-forwards (routing is bounded to one hop), so
a forward that lands on a follower errors "not the leader here" on every
retry with the same first-listed replica. Two sound test shapes: gate on
the *specific table* the node actually replicates (poll `/admin/status`'s
per-tablet `table` + `replicas` and pick that table for the
through-only-this-node ops), or give the client every node's address
(`cluster_growth.rs`'s round-robin `put`). The one-hop-blind-forward
behavior itself is a known production shape (the client is expected to
retry with fresh routing), not a bug this test should have papered over
with a longer timeout. (`animusd` `tests/seed_join.rs::table_with_replica`.)
**Generalizes beyond "just-grown node + arbitrary table" (ADR 0035 PR5):
ANY node with zero local replicas of anything hits the identical
fixed-non-leader-pick flake** — a control-only node (ADR 0035 PR3/PR4)
*structurally* never has a replica of any tablet, so a test asserting a
`Put`/`Get` succeeds through one fixed control node's client address alone
flakes on whichever of the tablet's replicas happens to win that
particular Raft election (a genuine ~50/50 for RF=2, not tied to growth/
rebalancing timing at all). Same fix: round-robin across every node's
client address, control **and** data, so the round-robin is guaranteed to
hit a node that resolves correctly (a real replica) even when the
control-node leg of the same loop lands on the wrong pick.
(`animusd` `tests/data_only.rs::split_cluster_serves_reads_and_writes_across_data_nodes`.)
**Update (hinted-retry forwarding, closes the hazard): `ClientCtx::cp_forward`
is now the single choke point every CP forward call goes through, and it
retries.** A "not the leader here" refusal (`cp_serve_forwarded`) now
carries the refusing (replica-hosting) node's own leader hint —
`topology::format_not_leader_refusal`/`parse_not_leader_refusal`, a plain
string suffix so old and new binaries still interoperate — and `cp_forward`
chases it: retry at the hint's address if untried, else at another of the
tablet's known replicas, bounded to one pass over {hint} ∪ replicas and to
the existing per-hop `CLIENT_TIMEOUT` budget for the whole sequence (not
per attempt). The one-hop invariant is unchanged — only the *forwarder*
retries, the receiver still never re-forwards. A node with zero local
replicas now resolves deterministically through a single fixed address, so
the round-robin test crutches above are no longer needed for *this*
hazard specifically (`tests/data_only.rs`/`tests/cluster_split.rs` reverted
to a fixed control-only node's address; `tests/cluster_split.rs::
fixed_control_node_write_read_is_deterministic` is the focused regression).
**Second update (user-hit live, one release later): a bounded retry pass
over {hint} ∪ replicas closes "wrong replica" but not "no leader YET" —
when every candidate refuses `leader_hint=none` (the whole group is
mid-election: a split-child/first-provision formation window, or a crashed
leader), giving up the moment one pass exhausts surfaces the refusal to
the client even though the election resolves within a couple hundred ms
and the deadline budget is barely touched.** `cp_forward` now backs off
`FORWARD_ELECTION_BACKOFF` (~one election timeout) and re-runs the pass,
still hard-bounded by the same overall `CLIENT_TIMEOUT` — the forwarded
dual of the local path's `RouteDecision::Wait`, which already waited out
its own group's election for exactly this reason. Gated on the tablet
being resolvable so an unmappable op keeps failing fast. General check
for any bounded retry-over-candidates loop: "every candidate refused" has
two distinct causes — *wrong candidates* (retrying the same set is
useless, return) vs. *right candidates, transient state* (they'll succeed
shortly, wait and re-ask) — and a loop that only handles the first
converts every instance of the second into a spurious client-visible
error. (`tests/cluster_split.rs::
single_shot_first_write_through_control_node_succeeds` — ONE un-retried
Put racing the provisioning/formation window.)
The general lesson stands for any *other* future one-hop-forward gap this
pattern doesn't cover.
