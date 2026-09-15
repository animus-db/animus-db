# A compiler-error-driven auto-fixer that blindly applies the *primary* span of a "mismatched types" diagnostic corrupts call sites where rustc's primary span is the *callee*, not the mismatched argument.

**A compiler-error-driven auto-fixer that blindly applies the *primary*
span of a "mismatched types" diagnostic corrupts call sites where rustc's
primary span is the *callee*, not the mismatched argument.** Turning
`NodeId` into a newtype (ADR 0040 PR2) meant sweeping ~300+
now-mismatched-argument call sites; scripting "wrap the primary span's
text in `nid(...)`" off `cargo build --message-format=json` worked for the
common single-bad-argument shape, but rustc collapses a call with **two or
more** simultaneously-mismatched arguments into one diagnostic titled
"arguments to this method are incorrect" whose *primary* span underlines
the **method name** (e.g. `partition_pair`) for the "where" location, with
each individual argument's mismatch riding a separate **non-primary**
span. Blindly wrapping the primary span turned `sim.partition_pair(1, 2)`
into `sim.nid(partition_pair)(1, 2)` — syntactically bizarre but *some*
of these slipped past a first pass because the corruption doesn't always
fail in the same file/build batch it was introduced in (a cascading parse
error elsewhere can suppress it from that round's diagnostics, so it only
surfaces once the parse error blocking it is separately fixed). **Fix:
detect the multi-argument shape from the diagnostic (rendered message
`"arguments to (this method|this function) are incorrect"`, or simply:
primary span text is an identifier matching the callee, not an
expression) and wrap each individual **argument's own non-primary span**
instead of the call's primary span.** A second, related hazard from the
same sweep: wrapping array/`vec!` literal elements one at a time from
per-element diagnostics can leave a literal **partially wrapped** (`[nid(0),
1, 2]`) if only the first element's mismatch was reported in a given pass —
always re-scan finished literals for a bare integer sitting next to an
already-`nid(...)`-wrapped sibling before considering a file done. General
rule for any future compiler-diagnostic-driven bulk rewrite: **verify one
layer down from "which span does the tool report as primary" before
trusting it as "the expression to edit"** — a fresh `cargo build
--workspace --all-targets --keep-going` after every batch of edits (not
just after the batch that seems to close out a file) is what caught these,
since a still-corrupted call site fails loudly (parse error or a
nonsensical type error) rather than silently. (ADR 0040 PR2, `animus-env`
`NodeId` newtype sweep — corruption found and fixed in `animus-sim`,
`animus-consensus`, `animus-cp-data`, and `animus-control` test files.)
