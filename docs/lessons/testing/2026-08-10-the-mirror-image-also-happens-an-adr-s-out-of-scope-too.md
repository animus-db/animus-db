# The mirror image also happens: an ADR's "out of scope, too large a refactor" call is a decision made against the codebase *as it existed then* — recheck it against what has actually shipped since, don't treat it as permanent.

**The mirror image also happens: an ADR's "out of scope, too large a
refactor" call is a decision made against the codebase *as it existed
then* — recheck it against what has actually shipped since, don't treat it
as permanent.** ADR 0030 §3 evaluated "a data node with no control role at
all" and rejected it as not viable "without a much larger refactor than
this slice warrants," because at the time `BoundNode::start_with` had a
hard structural requirement (every node owns a local `RaftCore`) with no
seam to route around it. Two ADRs later (0030's own growth-node
remote-metadata mirror, then ADR 0032's join/address-book work), the exact
mechanism that claim was missing had already been built and proven correct
in production — for a narrower reason each time (letting a non-voter
observe `Metadata` at all; keeping addresses live as the cluster grows) —
so designing ADR 0035 (control plane as a separate deployment) was mostly
recognizing that the "much larger refactor" had already happened
piecemeal, not writing it from scratch. **When starting any task, check
whether a prior ADR already declared a closely related shape out of scope —
if so, re-derive whether that scope decision still holds given every
feature that has shipped since, rather than treating the old ADR's "not
viable" as a permanent wall.**
