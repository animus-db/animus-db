# A removal ADR's own "every reference is updated" claim needs the same skepticism as any other ADR-vs-code drift — verify it, don't cite it.

**A removal ADR's own "every reference is updated" claim needs the same
skepticism as any other ADR-vs-code drift — verify it, don't cite it.**
ADR 0053 (2026-08-22, drop the CQL wire adapter) asserted a complete sweep:
"every `cql`/`CQL` reference across the workspace... is updated... or
reworded to note the history." An independent read found it false in the
load-bearing places that matter most — the two ADRs it explicitly claimed
to amend (ADR 0047's port-stride formula was never touched) plus two more
it didn't even claim (ADR 0052, ADR 0035, both still asserting a
"seven-port" stride as current fact with `cql` in the list), a live admin
endpoint documented as still-present after its own deletion (ADR 0020's
`POST /admin/data/cql`), a whole design-doc section for a UI panel deleted
from the actual dashboard (ADR 0021's CQL query panel), a crate `CLAUDE.md`
still listing a deleted `Metadata` field and a deleted `apply` arm, and a
pre-existing test assert string naming a `MetaCommand` variant
(`CreateKeyspace`) that no longer compiles anywhere else in the tree. **A
stale ADR that claims it swept is worse than one that says nothing**,
because a future reader trusts the claim instead of grepping to check it
themselves — the fix (this entry's own change) treats "amends ADR N" as a
testable assertion: either ADR N's own text now reflects the change, with
a dated amendment note in the ADR 0001/0019/0044 style, or the claim gets
softened to name what was actually swept (the structural, current-fact
references) versus what was deliberately left alone (the much larger body
of older ADRs' and the dated lessons log's own historical narrative
prose, which is *supposed* to keep describing a deleted feature as it
stood at each document's own date — rewriting those would launder history,
not fix it). **When asked to "sweep every reference to X," grep the whole
tree at the end and read every hit** — not just the files the task
description named — because a sweep claim is exactly the kind of thing
that silently narrows from "everything" to "everything I happened to
touch" without anyone noticing until an independent verifier greps it.
