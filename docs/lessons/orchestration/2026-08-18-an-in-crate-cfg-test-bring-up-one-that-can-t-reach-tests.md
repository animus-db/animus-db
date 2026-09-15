# An in-crate `#[cfg(test)]` bring-up (one that can't reach `tests/support`) needs the documented port-TOCTOU retry exactly as much as an external integration test does — the isolation from the shared helper doesn't exempt it from the race.

**An in-crate `#[cfg(test)]` bring-up (one that can't reach
`tests/support`) needs the documented port-TOCTOU retry exactly as much
as an external integration test does — the isolation from the shared
helper doesn't exempt it from the race.** `crates/animusd/src/cql.rs`'s
`cql_kind_write_tests::cql_whole_partition_delete_serves_from_every_node`
brought up a 3-node cluster with a single pass of `free_addrs` + `run_node`
per node and no retry, so it panicked `AddrInUse` under
`cargo test --workspace` contention (CI run referenced in issue #278 item
3) exactly like the pre-fix external tests this same lesson log already
covers. Swept every other in-crate mod with a hand-rolled bring-up for the
same gap: `cql_kind_write_tests::single_node`, `dynamo::
stream_write_path_tests::single_node`, `index_drain::
gsi_drain_cursor_tests::single_node`, `index_drain::stream_sealer_tests::
single_node_with_streams`/`single_node_with_streams_and_quiesce_after`/
`f11_end_to_end_auto_split_on_a_streamed_table_lands_a_token_aligned_boundary`'s
inline bind, and `confirm_futility_tests::single_node` — all fixed with the
same fresh-config-per-attempt bounded retry (16 attempts/50ms,
`tests/split_build.rs::bring_up`'s shape). `index_drain::
gsi_drain_cursor_tests::crash_mid_reconcile_recovers_without_skipping_or_corrupting_the_gsi`
needed *both* idioms in the same test: its **first** bring-up can and does
retry with fresh ports, but its **restart** rebinds the exact captured
config/dir (the property under test) so it retries the rebind itself on a
bounded wall-clock deadline instead, mirroring `tests/support/
mod.rs::restart_same_addrs`. **Followed the established per-mod-duplication
precedent already in this file** (each mod's own comment: "a different
compilation unit... duplicated rather than shared") rather than
introducing a new shared `pub(crate)` test helper, since the codebase had
already made that call for the surrounding `single_node`/`free_addrs`
fixtures these bring-ups live beside. **General rule: when auditing for a
known test-infra race, grep for the raw primitive it exploits (here,
every unretried `run_node`/`Node::bind` call reachable from a `#[cfg(test)]`
mod), not just the file the bug report named** — the same hand-rolled
bring-up shape gets copied wherever a private handle forces an in-crate
test module, and each copy is independently exposed.
(`crates/animusd/src/{cql,dynamo,index_drain,lib}.rs`.)
