# A commit message's claim that a test helper is "now dead" must be verified by grepping the helper's name across its whole file, not inferred from "the one test that motivated the deletion is gone."

**A commit message's claim that a test helper is "now dead" must be
verified by grepping the helper's name across its whole file, not
inferred from "the one test that motivated the deletion is gone."**
148b3ac (ADR 0044, removing tablet merge) deleted `streams_e2e.rs`'s
local `admin()` helper alongside the one merge-stopgap test that called
it, reasoning it was now unused — but three *other* tests already in
that file (`admin_status_survives_a_populated_stream_shard_catalog`,
`admin_data_dynamo_proxy_reaches_streams_read_api`,
`admin_data_dynamo_proxy_rejects_unknown_op_cleanly`) also called it,
so the deletion broke `cargo build --workspace --all-targets`/`cargo
test -p animusd` outright — silently, since this repo's CI billing is
broken (see the `docs/engineering-lessons-archive.md`/root `CLAUDE.md`
history) and no automated gate ever ran it after merge. Sat unnoticed on
`main` until the next agent to touch `animusd` hit it cold. Found and
fixed while building ADR 0045 PR1 (index-status catalog plumbing,
unrelated) — restored the helper verbatim from before the deletion
commit. General form: when deleting a symbol because "its only caller is
going away," grep the symbol's own name (not just its caller) across the
file/crate before deleting it, exactly the same discipline this log
already prescribes for gating `match` sites on a command enum.
(2026-08-15.)
