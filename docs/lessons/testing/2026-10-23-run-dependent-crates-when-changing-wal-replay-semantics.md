# Changing WAL replay semantics: run the crates that replay WALs, not just the owner

Issue #1368 made `PersistedState::replay` treat a `Snapshot` record as a log reset. `cargo test -p animus-control`
and `-p animus-cp-data` passed, but `animus-test`'s `upgrade_restart_tier0::control_wal_fixture_restarts_with_current_code`
failed: the checked-in `control-wal` fixture is a synthetic "one of each record" file with a `Snapshot` AFTER an `Append`,
an order no binary ever wrote, and the test pinned the replayed log from it.

Why it matters: the fixtures are immutable (ADR 0073), so a replay-semantics change surfaces as a changed per-version
expected value in the tier-0 test, not as a fixture edit. Before pushing a change to `replay`/decoders, run
`cargo test -p animus-test` (tier 0/1 upgrade-restart, raftkv corpora), not only the owning crates, and state in the test
comment why the changed expectation is safe (the records still decode identically; only a never-written order replays differently).
