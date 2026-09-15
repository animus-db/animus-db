# A converged-or-timeout poll's own new timeout must be measured against the real environment, not borrowed from a nearby-but-different-purpose constant (issue #670/#622, `split_placing_two_replica_diff_e2e.rs`)

Fixing the first failure mode of issue #670 (the #622 mechanism) meant
replacing a one-shot assert on the freshly-provisioned parent tablet's
initial replica set with a converged-or-timeout poll, per the root
`CLAUDE.md`'s standing rule. The first cut picked its own poll's budget
by copying the nearest-looking existing value in the file —
`Duration::from_secs(20)`, used two lines away for `await_bootstrap`'s
cluster-bootstrap bound and for `await_all_active`'s growth-node
liveness bound. Both of those are the wrong reference class: they bound
a *fixed, usually-fast* startup step, not "let a busy real cluster
converge under load," which is what RF self-heal (this poll's actual
subject) is. Running the fixed test 10x in this sandbox measured 2 of
10 runs still short of 3 replicas at the 20s mark — the new poll's own
timeout fired and asserted failure on a placement that was still
legitimately converging, a self-inflicted flake in the very fix meant to
remove one. Widening the poll's own budget to 60s (matching this same
file's `join_extra`/`await_cutover_of` budgets, both already used for
"real cluster, real load, genuinely eventual" waits) cleared it: a
follow-up 10x run passed 9/10, the one failure being an unrelated,
already-documented issue #670 mode (`voter_history` never reaching the
directed-Placing target — the placement-oscillation mode, at
`assert_eq!` line ~718, not this fix's own assertion at all).

**General form**: when adding a new converged-or-timeout poll, don't pick
its budget by matching a same-magnitude constant already sitting nearby
in the file — check what that existing constant is actually bounding.
Two `Duration::from_secs(20)`s five lines apart can guard entirely
different kinds of waits (a usually-instant fixed step vs. a
genuinely-eventual one under load), and reusing the wrong one's number
just relocates the "fixed-deadline one-shot assert on an eventual
property" bug one level down, into the new poll's own timeout, where the
next flake report will look identical to the one just fixed. Measure the
new poll's own budget against a real run of the environment it will
actually execute in (a loaded sandbox, a shared CI runner), not against
whatever number happens to be typed a few lines up.
