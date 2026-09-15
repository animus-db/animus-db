# Recording a policy derived from a point-in-time observation makes a transient condition permanent — record the *target*, and let reconciliation close the gap from observation to intent.

**Recording a policy derived from a point-in-time observation makes a
transient condition permanent — record the *target*, and let
reconciliation close the gap from observation to intent.**
`provision_tablet` conflated two different things that happened to be
computed from the same data at creation time: the tablet's *initial*
replica set (legitimately best-effort — however many candidates are
`Active` right now) and its *policy* (a durable, ongoing commitment to a
desired state). Deriving the policy from the initial set's observed size
meant a transient "not everyone has promoted yet" moment got baked in
forever, because nothing ever re-derives an already-recorded policy from
a fresher observation. The fix wasn't reading fresher data (that was
already tried once for this exact call site — see the `metadata_fresh()`
entry above — and only narrowed the window, it didn't close it, because
*any* read, however fresh, can still land inside a real convergence-in-
progress). The fix was to stop deriving the policy from an observation at
all: record the fixed target, and lean on the reconciler's existing
violation-repair path (already proven correct for "a replica died,
replace it") to grow an under-sized initial set the moment reality
catches up to the target. General check: when code sets a persistent,
non-retried field from "whatever I can currently observe," ask whether
that quantity is supposed to be an *intent* (should stay fixed regardless
of when it's read) or a *snapshot* (fine to vary with timing) — and if
it's an intent, a downstream repair loop must be able to re-derive and
close the gap, not just react to future violations of whatever got
recorded first.
