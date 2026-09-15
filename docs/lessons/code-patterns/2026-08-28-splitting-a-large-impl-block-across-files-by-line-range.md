# Splitting a large `impl` block across files by line-range extraction: a naive "walk upward from the `fn` signature, stop at the first line that isn't a doc comment" heuristic silently orphans multi-line and trailing-comment attributes, and the resulting compile error points at the wrong function (ADR 0061 rung C5 step 2, `animusd`'s `ClientCtx` split into `schema.rs`/`read_path.rs`/`write_path.rs`/ `txn_coordinator.rs`/`forwarding.rs`).

**Splitting a large `impl` block across files by line-range extraction:
a naive "walk upward from the `fn` signature, stop at the first line
that isn't a doc comment" heuristic silently orphans multi-line and
trailing-comment attributes, and the resulting compile error points at
the wrong function (ADR 0061 rung C5 step 2, `animusd`'s `ClientCtx`
split into `schema.rs`/`read_path.rs`/`write_path.rs`/
`txn_coordinator.rs`/`forwarding.rs`).** Two attribute shapes break a
naive upward walk that only recognizes `///`/`#[...]` (single-line,
balanced) directly above a signature: a **multi-line** attribute
(`#[tracing::instrument(\n    name = "...",\n    skip(self),\n)]`,
rustfmt's own style once an attribute's arguments don't fit one line)
and a **single-line attribute with a trailing line comment**
(`#[allow(clippy::too_many_arguments)] // mirrors ClientRequest::...`).
Neither matches "line starts with `#[` and ends with `]`", so the walk
stops one line short, extracts everything from the signature down but
leaves the attribute behind — and because the attribute is syntactically
valid on its own, it silently reattaches itself (per Rust's "doc
comments/attributes attach to the next item" rule) to whatever function
ends up sitting below the extracted one after the move. The resulting
error is **not** "attribute not found" at the extraction site — it's
`error: expected item after attributes` if nothing follows, or a
`too_many_arguments`/wrong-`#[allow]` clippy failure on an unrelated
function if something does — neither of which points at the actual
mistake. Fix: track bracket balance (`(`/`)`/`[`/`]`, since `#[attr(args)]`
mixes both) walking upward from a line that looks like an attribute's
*tail* (matches `^\)*\]\s*,?\s*(//.*)?$`) until the balance returns to
zero at a line that starts with `#[` — that's the true start; verify by
re-grepping the moved chunk and the source file afterward for the
attribute's own text (`#[allow(clippy::` or `#[tracing::instrument`,
whatever the file actually uses) rather than trusting the tool's line
count alone. The corollary compiler-privacy lesson from the same
rung: this codebase's module tree makes every file a *descendant* of
`lib.rs` (the crate root), so a formerly-bare `fn` (no `pub`/`pub(crate)`)
that only needed to be reachable from sibling files staying in `lib.rs`
needed **no** visibility change to keep compiling once callers moved
into new child modules — Rust's privacy rule is "visible in the
defining module and its descendants," and a parent's private items stay
visible to every descendant regardless of which descendant. The
widening was only needed in the other direction: a method that itself
moved into a new child module (`schema.rs` et al.) and is called from a
*sibling* child module or from code that stays in the parent needs
`pub(crate)` — 30 of the ~83 relocated methods needed it for exactly
that reason (`cp_serve_forwarded`, moved last, turned out to be the
single biggest source of these: it calls into every other cluster by
name). `cargo build`'s `E0624: method \`x\` is private` errors name the
exact call site, so the mechanical move-then-build-then-widen loop
catches every miss — but only if each intermediate build is actually
run, since a batch of moves without an intervening build can mask one
widening behind another's cascading errors.
