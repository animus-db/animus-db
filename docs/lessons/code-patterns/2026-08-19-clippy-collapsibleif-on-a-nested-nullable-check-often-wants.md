# `clippy::collapsible_if` on a nested nullable check often wants an `if cond && let Some(x) = opt { .. }` let-chain, not a manual flatten (2026-08-19).

**`clippy::collapsible_if` on a nested nullable check often wants an
`if cond && let Some(x) = opt { .. }` let-chain, not a manual flatten
(2026-08-19).** `describe_time_to_live_response`'s `if enabled { if let
Some(name) = attr { .. } } }` tripped `collapsible_if` under `-D warnings`;
clippy's own suggested fix (`if enabled && let Some(name) = &attr { .. }`)
compiles cleanly on this workspace's toolchain (let-chains are stable
here) and is both shorter and more direct than restructuring the logic by
hand — read the lint's `help:` suggestion before reaching for a manual
rewrite, it's frequently already the answer.
