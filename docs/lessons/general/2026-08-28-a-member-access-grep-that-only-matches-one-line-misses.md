# A member-access grep that only matches one line misses `receiver\n.method()` sites rustfmt wrapped onto two lines (ADR 0061 rung C4d)

Scoping the `AdminHost` capability trait (rung C4d) started from a
single-line regex — `grep -noE "ctx\.[a-zA-Z_]+" admin.rs` — to enumerate
every `ClientCtx` member `admin.rs`'s handlers actually touch, the same way
earlier rungs' scoping passes had. It found 14 members and matched the
ADR's own "a 15-method cluster-shape slice" estimate closely enough to look
confirmed. It wasn't: `ctx.stream_change_rates()` and `ctx.trigger_split(..)`
were invisible to it, because rustfmt had wrapped both call sites as

```rust
let stream_change_rates: Vec<Value> = ctx
    .stream_change_rates()
```

— `ctx` and `.stream_change_rates()` on separate lines, so a pattern
anchored on `ctx\.` never sees the method name at all. A second pass that
first collapsed `re.sub(r'ctx\s*\n\s*\.', 'ctx.', text)` before matching
found 19 members, not 14 — `admin_add_control_member`,
`admin_remove_control_member`, `cp_kind_write_item`, `stream_change_rates`,
and `trigger_split` had all been missed, some of them (`cp_kind_write_item`,
`trigger_split`) real write-path primitives whose absence from a
capability-trait scoping pass would have been a correctness gap, not just a
miscount, had it gone unnoticed and the trait shipped one method short.

This is worth being deliberate about any time a task's *validation*, not
just its implementation, rests on grepping a large `rustfmt`-formatted
file for one identifier chained off another (`x.method()`,
`self.field.method()`): rustfmt line-wraps a chain at ~100 columns with no
regard for keeping the receiver and the first `.` on the same line, so a
single-line regex silently undercounts on exactly the call sites long
enough to wrap — which correlates with exactly the call sites most likely
to be doing something nontrivial. Either collapse whitespace-then-dot
before matching (as above), or use a tool that already understands Rust
syntax (`cargo expand`, an AST-aware grep, or simply reading the file)
rather than trusting a plain-text line-oriented pattern to enumerate a
type's own usage.
