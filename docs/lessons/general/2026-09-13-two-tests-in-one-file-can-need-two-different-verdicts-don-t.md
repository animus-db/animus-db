# Two tests in one file can need two different verdicts — don't let a file-level open question force an all-or-nothing answer (ADR 0061 rung M, C-13 PR 6)

`control_membership_split.rs`'s own conversion was left as one open
question by the opener plan: build a new "introduce a control-bearing
non-voter node" primitive (option a), or route it through the seed/join
dial (option b), or — if neither proves tractable — assess-and-close the
whole file. All three framings implicitly treated the file's two tests as
one decision. Reading each test's own body directly, independently, before
picking a framing showed they are not the same kind of test at all: one
(`admin_add_control_member_races_a_control_only_self_registration_and_
still_converges`) never touches `Node::bind_control`/discovery/claim at
all — its own real subject is a pure control-plane admin-vs-apply-task
timing race, provable with a fabricated `NodeAddrs` and a direct leader
propose, both mechanisms already fully generic and already reachable; the
other (`grow_then_replace_a_voter_over_a_split_deployment_with_live_data_
traffic`) really does need the deferred "combined growth" primitive. Once
read separately, there was no single verdict to force — one converts
cleanly with a one-line accessor addition, the other stays real-socket,
precisely and narrowly named. **The general rule**: an open question
scoped at the file level by a prior investigation is a starting point for
where to look, not a commitment to answer at that granularity — re-derive
the unit of decision (per assertion, per test, or per file) from what the
tests actually do, the same "don't propagate an inherited label" discipline
this log already records for single tests and roadmap inventories, applied
here one level up to a *grouping* decision instead of a single test's own
label.

**A second, related finding from the same PR: two independent production
doc comments naming the identical gap is corroborating evidence worth
citing verbatim, not a reason to skip verifying it yourself.**
`SimCluster::grow`'s own doc (this rung's own PR 2's template method) and
`sim_cluster_control_membership_admin.rs`'s own module doc (a separately
landed prior rung, C-12 PR 4e) both independently state that a genuinely
new control-plane-voter growth node is deferred, materially-different,
separately-budgeted machinery — arrived at from two different angles (one
building the seed/join dial, one converting the closest sibling
real-socket file) rather than one comment copying the other. That
independent convergence is strong evidence the gap is real and not an
artifact of either PR's own local framing — but the conversion still
should not stop at citing both quotes; this PR re-derived the SAME
conclusion from the actual mechanism (`self.controls: Vec<RaftNode<
SimEnv>>` never growing post-construction, `RaftCore`'s own learner
machinery not being what the real test's `join_control_nonvoter` shape
needs since that helper's own node starts genuinely OUTSIDE the group's
config, not as a tracked learner inside it) before treating the two prior
doc comments as confirmation rather than as the whole argument. Citing a
predecessor's conclusion without re-deriving it independently is exactly
the failure mode ADR 0061's own "Rung L closed" amendment already named
for a different case (a rung's inherited "why it stays `ProdEnv`" label
turning out to be stale once actually re-read) — the fix generalizes: two
independent citations agreeing is good evidence, never a substitute for
checking the mechanism yourself.
