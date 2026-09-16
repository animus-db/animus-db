# A "blocker (d)" residual can close itself the moment the product PR that fixes it lands — don't assume the residual needs its own product change too

**A "blocker (d)" residual can close itself the moment the product PR that
fixes it lands — don't assume the residual needs its own product change
too** (2026-09-09, ADR 0061 rung J, C-10 PR 6). C-08 PR 2 left `console_
table_config.rs`'s three GSI-DDL tests `ProdEnv` because `GenericConsole
Backend::add_gsi`/`drop_gsi` (already generic) fell through `dispatch_
table_op`'s `UpdateTable` arm into `unsupported_by_generic_dispatch` —
the console methods themselves were never the blocker, the dispatch gap
they routed through was. C-10 PR 2, two rungs later and written for an
unrelated set of `tests/update_table_*.rs`/`dynamo_gsi_drain.rs` files,
closed that exact dispatch gap as groundwork. By the time PR 6 picked up
the `console_table_config.rs` residual, converting it needed **zero**
`lib.rs`/`console.rs`/`dynamo.rs` change — pure test authorship reusing a
product fix two PRs old. The general lesson: when a kept-`ProdEnv` test's
own doc comment names a specific blocker ("blocker (d)", "no dispatch
arm for X", "falls to `unsupported_by_generic_dispatch`"), re-check
whether a *later, unrelated-looking* PR already closed that exact named
blocker before assuming the residual still needs product work — grep the
blocker's own symptom (the dispatch function's `match` arms, the error
string) rather than trusting the kept test's own stale-by-now framing of
"this needs X built first."
