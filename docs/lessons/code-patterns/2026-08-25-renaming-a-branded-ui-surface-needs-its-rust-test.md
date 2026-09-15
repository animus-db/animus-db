# Renaming a branded UI surface needs its Rust test *assertions* fixed to stay green, but its doc-comment prose is a separate, lower-priority sweep — don't conflate the two passes (2026-08-25, ADR 0056, admin/console rename).

**Renaming a branded UI surface needs its Rust test *assertions* fixed to
stay green, but its doc-comment prose is a separate, lower-priority sweep
— don't conflate the two passes (2026-08-25, ADR 0056, admin/console
rename).** Renaming the operator dashboard's brand text ("AnimusDB
Console" → "animusd admin") and the data app's ("AnimusDB Data Console" →
"animusd console") broke exactly two integration-test files
(`dashboard_endpoint.rs`, `console_endpoint.rs`) whose `body.contains(...)`
assertions checked the old literal strings — found by grepping `tests/`
for the old names *before* editing the HTML, per this repo's standing
rule, then fixed alongside the HTML in the same change so the gate never
went red. Dozens of *other* hits for the same old strings remain, on
purpose: module-doc `//!` comments and inline comments across
`dashboard.rs`, `dashboard_core.js`, `console.js`, and every
`crates/animusd/tests/console_*.rs` file's own header comment. None of
those are asserted by any test (confirmed by grep), so they don't fail
the gate — but they are exactly the "stranded documentation" class this
log already names (see the `ReplicationMode`-removal entry above): prose
that now describes a surface by a name the code no longer uses, silently,
with nothing failing to point at it. Left as a deliberate, separate
follow-up (out of this change's stated file scope) rather than folded in,
since a partial prose sweep across a ~2000-line crate guide risks
introducing exactly the kind of drift it would be fixing. **General
rule**: when a rename's brief says "update every asserted string," that
is a narrower, harder requirement than "update every occurrence" — grep
for assertions specifically (`.contains(`, `assert_eq!` against the
literal, etc.) to find the must-fix set, and treat every remaining
prose hit as a tracked, intentional gap rather than either silently
ignoring it or scope-creeping the change to chase it down.
