# A `RaftCore::new`/`RaftNode::start` bootstrap's `all_nodes` argument sets the node's own local `config` directly at construction, with no consensus involved at all — a test that includes a not-yet-added node's own id in its own `all_nodes` is trivially, unconditionally wrong from its very first line, regardless of any later replication (2026-08-24, ADR 0058 Train 1's learner corpus).

**A `RaftCore::new`/`RaftNode::start` bootstrap's `all_nodes` argument sets
the node's own local `config` directly at construction, with no consensus
involved at all — a test that includes a not-yet-added node's own id in its
own `all_nodes` is trivially, unconditionally wrong from its very first
line, regardless of any later replication (2026-08-24, ADR 0058 Train 1's
learner corpus).** This bit despite `animus-cp-data/CLAUDE.md` already
documenting the exact gotcha ("pre-start a to-be-added node knowing only
the *current* voters, NOT itself") — reading the warning and then still
writing `RaftNode::start(env, [0,1,2,3]..)` for the node with id 3 happened
because the assertion that would have caught it fired *after* the real
`add_learner`/`change_membership` had already replicated, at which point
the (buggy) locally-bootstrapped value and the (correct) replicated value
are byte-identical and the bug is invisible. It only surfaced because this
corpus asserted the learner's own `config()` *before* any real membership
change — `learner.config()` showed `{n0,n1,n2,n3}` immediately after
`RaftNode::start` returned, before the leader had even proposed
`add_learner`. **General rule**: when a test's own assertion checks a
freshly-started node's config/role *before* the operation under test has
had a chance to replicate anything to it, the "excluded from its own
`all_nodes`" gotcha becomes load-bearing rather than cosmetic — and a
pre-existing test that only asserts *after* replication (e.g.
`animus-control/tests/control_membership.rs`'s own `add_a_node_...` test,
which includes the new node's id in its own bootstrap set and gets away
with it) is not proof the risky pattern is safe in general, only that its
own assertions never exercised the window where it would matter.
