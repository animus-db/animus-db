# A small cluster where every node hosts every tablet (`n == RF`) can silently mask a cross-tablet routing bug — a "foreign" record lookup that should require genuine cross-node routing can instead succeed by accident via same-node engine co-location.

**A small cluster where every node hosts every tablet (`n == RF`) can
silently mask a cross-tablet routing bug — a "foreign" record lookup
that should require genuine cross-node routing can instead succeed by
accident via same-node engine co-location.** `RaftKvNode::resolve_once_
step`'s "is this transaction's record local" check reads the record's
physical bytes directly off `self.storage` using the *querying* tablet's
own `StorageScope` prefix — which is safe and correct precisely because
ADR 0028 puts every tablet of one table's replicas on the *same node*
under one shared engine and prefix, so a "local" read really does mean
"this replica's own copy." But that same design means that on a
3-node/RF-3 cluster (the default `bring_up(3, ..)` shape most `animusd`
integration tests use), literally every node hosts every tablet of
every table — so a tablet that does *not* logically own a key can still
physically read another tablet's record through the identically-prefixed
shared engine on the same node, purely because nothing about placement
spread them apart. A wire-level regression for this exact write-loss bug
(`animusd/tests/txn_recovery_participant_spans.rs::recovery_resolve_
correctly_commits_both_tablets_of_a_two_tablet_transaction`) was found
to pass identically whether or not the coordinator-side grouping fix was
present, for exactly this reason — a real fail-before/pass-after
demonstration of *that specific fix* needs a cluster large enough to
force the anchor's and participant's tablets onto genuinely disjoint
replica sets (well beyond a 3-node default), or a lower-level
`animus-cp-data` `SimEnv` test that constructs the shared-engine shape
directly without relying on real placement (which is what this
incident's actual fail-before/pass-after evidence uses instead — see the
entry above). **When a wire-level integration test is meant to prove a
cross-tablet/cross-node property, check whether the cluster's own size
relative to the replication factor could make "every replica has every
tablet" true** — if so, the test may still be a useful regression, but
it cannot discriminate the specific bug it was written to catch, and
that gap should be documented rather than assumed away. (Torn-pair-fix
stack PR3, 2026-08-15.)
