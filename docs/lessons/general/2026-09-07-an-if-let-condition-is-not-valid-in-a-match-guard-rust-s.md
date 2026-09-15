# An `if let` condition is not valid in a match guard — Rust's let-chains stabilization covers `if`/`while` only (S-07e, `animus-operator` controller.rs)

This repo's own code already leans on let-chains heavily (`if let Some(x) =
a && let Err(e) = x.validate() { .. }`, throughout `controller.rs`), which
made it tempting, while wiring `crate::validate::control_nodes_regression`
into an existing `match prior_control_nodes { Some(prior) if .. => .. }`
arm, to reach for the identical shape as a match guard: `Some(prior) if
let Some(violation) = validate::control_nodes_regression(prior, target) =>
{ .. }`. That is a *different* language feature (`if_let_guard`, tracking
issue #51114) that has never stabilized — only a plain `if`/`while`
condition got let-chains in this edition, never a match arm's own guard
clause. The mistake was caught by re-reading the diff before compiling,
not by a build failure, but it is exactly the kind of "this codebase does
this shape everywhere, so it must work everywhere" reasoning worth naming:
a stabilized language feature's scope is per-*construct*, not a blanket
grant to every syntactic position that resembles it. Fixed by keeping the
match's own boolean guard (`Some(prior) if validate::
control_nodes_regression(prior, target).is_some() =>`) and re-deriving the
`Violation` inside the arm body via a second call (documented as
deliberately redundant, not a bug) rather than trying to bind it in the
guard itself. Before reaching for a let-chain in a match guard anywhere in
this codebase, restructure into a plain `if`/`while` (or an `if`-then-
`match` split) instead — it will not compile on this toolchain.
