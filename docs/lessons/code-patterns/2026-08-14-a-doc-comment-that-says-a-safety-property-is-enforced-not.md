# A doc comment that says a safety property is "enforced, not assumed" is a claim about the code, not a substitute for grepping it.

**A doc comment that says a safety property is "enforced, not assumed" is
a claim about the code, not a substitute for grepping it.** Both ADR 0041
§1 and `animus-dynamo/src/index.rs`'s own doc for `INDEX_TABLE_SEPARATOR`
state that `$` is illegal in a user table name and that this is enforced
at `Metadata::apply`'s `CreateTableSchema` arm, "alongside the existing
`syskv::is_reserved_name` gate." Re-grounding the DynamoDB Streams work
(ADR 0042/0043) in what the tree actually enforces required grepping
every `is_reserved_name`/`$`/`INDEX_TABLE_SEPARATOR` call site — and
turned up that the `$` rejection the ADR and the code comment both
describe **did not exist anywhere in the codebase**. `CreateTableSchema`'s
apply arm only ever checked `is_reserved_name`; no code path rejected a
`$`-containing table name. The design had been sound in principle the
whole time (nothing had ever proposed a `$`-named table, so no real
collision had occurred), but "sound in principle, unenforced in practice"
is exactly the gap a design that leans on the same unverified assumption
could turn into a real, reachable, unenforced case. Fixed at the single
call site both docs already named. The general move: before building
anything whose own correctness argument leans on a property another
ADR/doc claims is "enforced," verify it holds *now*, at the call site the
doc names — "already enforced elsewhere" is a claim to check, not a fact
to inherit, even (especially) when it's stated confidently in the very
document you're extending. (`crates/animus-control/src/meta.rs::
CreateTableSchema`; ADR 0041 §1's as-built correction, ADR 0042/0043
round-3 streams salvage, 2026-08-14.)
