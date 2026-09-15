# An integration test that greps a served JS asset for an exact literal (`core_js.contains(r#"data: [...]"#)`) is asserting the string, not the behavior it happens to encode today

**An integration test that greps a served JS asset for an exact literal
(`core_js.contains(r#"data: [...]"#)`) is asserting the string, not the
behavior it happens to encode today** — it goes red the instant a later
change legitimately edits that literal, with no way to tell "this broke
the gating logic" apart from "this is exactly the intended new tab list"
from the failure alone; the fix is always to update the literal to match
the new intended list, never to treat the failure as a signal something
is wrong. Hit adding a Streams tab to `dashboard_core.js`'s `ROLE_TABS`
(ADR 0042/0043's Console follow-up): `dashboard_endpoint.rs`'s
`dashboard_role_gating_split_deployment` asserted the data role's tab
list was exactly `["node", "browser"]`, so appending `"streams"` to that
array failed the test even though the new list was exactly the intended
change. Not worth restructuring away (a substring match still proves the
asset actually ships the gating logic, which is the property ADR 0021 §6
wants — "the JSON/JS it renders is the tested JSON/JS," not a new
correctness proof) — just keep it front-of-mind when grepping a
gate-failure diff against a change that touches one of these string
literals: check whether the assertion's own expected string needs
updating before assuming the change broke something. (2026-08-14.)
