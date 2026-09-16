# A sparse-looking diagnostic log is a hypothesis to check, not evidence the writer is broken

Found investigating issue #864's rollout-completion wait (`animus-operator`,
`scripts/e2e-kind.sh`): a captured operator log that looked implausibly
short relative to the timeline it was supposed to cover, on more than one
real occurrence.

## The trap: "the log is too short" has several causes, and the boring one is usually right

A stalled reconcile whose own diagnostic log dump shows only a handful of
lines invites the same short list of exciting explanations every time: a
non-blocking/buffered writer silently dropping events, the process having
restarted (and the new instance's own early lines being all that
survived), or a genuine gap in what the code logs. All three are checkable
directly and cheaply, and none of them turned out to be true here: `main.rs`
uses `tracing_subscriber::fmt::init()`'s plain default writer — synchronous,
line-flushing, over `std::io::stdout` regardless of whether the destination
is a terminal or a redirected file — and the launching script `exec`s the
binary exactly once with a single, never-truncated `>` redirect. The
actual cause was the least exciting one: the diagnostics dump's own
`tail -n 200` on a log this crate can legitimately fill quickly (`owns()`
watching five child kinds means a single reconcile's own re-apply of all
five can itself trigger a burst of "related object updated" reconciles —
six inside 200ms was observed live), so a multi-minute stall accumulates
past that cap and the tail silently keeps only the *end* of the story,
discarding the middle where the interesting reconcile actually happened.

## The generalizable rule

Before trusting "the log doesn't show what I expect" as evidence about
the *system under test*, rule out the *diagnostic pipeline itself*: is
the writer synchronous or does it have a lossy buffer/queue in front of
it; did the process actually stay alive the whole window (a liveness
check costs one line); and is whatever bounds the capture (a `tail -n N`,
a ring buffer, a truncating rotate) sized for the busiest case this
component can produce, not the typical one. Each of these is a fact you
can pin down in isolation, cheaply, and each one you rule out narrows the
search — but skipping this step means every future investigation of the
same symptom re-litigates "is the log even trustworthy" from scratch
instead of building on an already-settled answer. Once the pipeline is
confirmed trustworthy, make it self-evidently so in the diagnostic output
itself (here: print the log's own total line count and the process's own
liveness right before dumping it) — a future stalled run should never
need this same investigation repeated to answer "was anything lost."

See ADR 0060's 2026-09-16 amendment (Part E) and issue #864 for the full
investigation, and issue #913 for the real, separate bug (a TLS
certificate-lifecycle race) this log-capture investigation cleared the
way to actually find.
