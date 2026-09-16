# A unit-level fix to a pure predicate proves nothing about its production call sites if a caller reconstructs the predicate's input in a way the predicate's own type dispatch can't see through — always trace the actual value a real caller hands the fixed function, not just the type it accepts in principle (2026-08-24, issue #373 follow-up, `animus-dynamo::condition` / `animusd::dynamo`).

**A unit-level fix to a pure predicate proves nothing about its production
call sites if a caller reconstructs the predicate's input in a way the
predicate's own type dispatch can't see through — always trace the actual
value a real caller hands the fixed function, not just the type it accepts
in principle (2026-08-24, issue #373 follow-up, `animus-dynamo::condition`
/ `animusd::dynamo`).** The entry above fixed `SortKeyCondition::matches`
to compare `N` numerically once *both* sides are literally the `N`
variant — and its own unit tests, which construct both sides as
`AttributeValue::N`, genuinely proved that. But every production caller
(`run_base_query`/`run_gsi_query`/`run_lsi_query` in `animusd`, and this
crate's own `Table::query_with`) held only a scanned key's **raw bytes**,
with no type tag, and wrapped them as `AttributeValue::B` before calling
`matches` — so the numeric arm's `(N, N)` pattern match never fired at any
real call site, even after the "fix" landed: `sort_key_cmp` fell through to
`a.key_bytes().cmp(&b.key_bytes())`, which for a `B`-wrapped raw-bytes
value is byte-identical to the *unfixed* behavior, since a `N`'s raw
stored bytes are literally its own decimal text. Confirmed empirically
(not just by code reading) with a throwaway `#[test]` calling `matches`
once with the value typed `N` and once with the identical bytes wrapped
`B`: the two calls returned different answers for the exact same logical
comparison. The general check: after fixing a comparison predicate that
dispatches on an enum variant (here, `AttributeValue`'s `N` vs `B`), grep
every production call site and ask "does this caller actually have a
value of the variant my fix's fast path checks for, or does it have raw
bytes / a different representation that only happens to satisfy the type
the function *accepts*?" — a function accepting `&AttributeValue` gives no
static guarantee the caller passes the semantically-correct variant, and a
fix's own unit tests, if they construct inputs "the right way" rather than
the way production actually does, can pass while production stays broken.
Fixed by adding `SortKeyCondition::matches_raw(&self, raw_bytes: &[u8])`,
which reinterprets raw bytes as the condition's own declared operand type
before delegating to `matches`, and switching every raw-bytes call site to
it — so the type-correct reconstruction happens once, in the one place
that knows the rule, instead of being (mis)implemented ad hoc at each
call site. (`crates/animus-dynamo/src/condition.rs`,
`crates/animus-dynamo/src/lib.rs`, `crates/animusd/src/dynamo.rs`.)
