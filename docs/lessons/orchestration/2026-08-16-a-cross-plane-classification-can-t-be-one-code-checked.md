# A cross-plane classification can't be one code-checked table when the dependency direction is one-way

**A cross-plane classification can't be one code-checked table when the
dependency direction is one-way** (the consumer-offset consolidation, ADR
0046's third amendment, 2026-08-16). `SplitPolicy`'s classification table
has three entries — `"gsi"`, `"backfill:{index_name}"` (both
`animus-cp-data::cursor`, a `KIND_CURSOR` row) and the stream seal
watermark (`animus-control::Metadata`, a replicated field, not a cursor
row at all). `animus-cp-data` cannot depend on `animus-control` (data
plane never depends on control plane) or vice versa in a way that would
let one function classify all three and have a compiler-checked test
cover all three uniformly — a real registry spanning both would be the
over-unification the approved plan explicitly rejected. The resolution:
put the code-checked enumeration (`classify_tag` + a test asserting every
*known* tag this crate constructs maps to a policy) in the lower crate,
covering only what that crate can see, and add the cross-plane member as
a **doc-level-only** row in the same table with an explicit note on why
it has no corresponding code check. This is weaker than a single
compiler-enforced table — a human, not the compiler, is the safety net
for the doc-level row staying in sync — but it is the honest version of
what a one-directional dependency graph allows, and it is strictly better
than the alternative of not naming the third case at all. **General
rule**: when a concept genuinely spans two components with a one-way
dependency, don't force a single shared type/table across the boundary —
split the enumeration at the boundary (code-checked on the side that can
see everything relevant, doc-level-only for the rest) and say so
explicitly in both the code comment and the table itself, rather than
quietly under- or over-claiming what's actually verified.
