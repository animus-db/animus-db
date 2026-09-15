# A test using the same bring-up helper as a known-in-scope test is not the same claim as the test exercising the same mechanism — trace what the test actually asserts, not what it sets up to reach the assertion (ADR 0061 rung N, C-14 PR 1)

`heartbeat_live_destinations.rs::heartbeat_reaches_a_runtime_added_voter_
after_it_becomes_leader` uses `join_control_nonvoter`, the identical helper
`control_membership_split.rs` uses — an obvious first guess that it would
convert the moment the combined-voter-growth primitive lands. It would not:
its own real subject is `heartbeat_loop_live` itself, a `ProdEnv`-hardcoded
background loop `SimCluster` has never used (it deliberately spawns the
plain, generic `heartbeat_loop` everywhere instead), plus `ProdEnv::merge_
peer`'s own per-env peer-book scope limit — both orthogonal to whether a
fresh voter can be grown under `SimEnv` at all. The general form: "calls the
same bring-up helper as a known-in-scope test" answers a different question
than "tests the same mechanism as a known-in-scope test" — the identical
distinction this crate's own C-13 template already recorded for
`join_data_seed_settings_reach.rs` ("does this file call the mechanism
under investigation" vs. "is the mechanism under investigation what this
file's assertions are actually about"), now confirmed a second time on an
unrelated file family. Always trace the test's own asserted property before
counting it as either in-scope or unblockable by a candidate primitive.
