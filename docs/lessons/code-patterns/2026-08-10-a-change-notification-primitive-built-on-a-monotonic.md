# A change-notification primitive built on a monotonic watermark re-checked fresh on every poll, instead of a one-shot consumed flag, eliminates the wake-before-park race class by construction — no special-case handling needed.

**A change-notification primitive built on a monotonic watermark re-checked
fresh on every poll, instead of a one-shot consumed flag, eliminates the
wake-before-park race class by construction — no special-case handling
needed.** `animus-cp-data`'s `ProposeSignal` (wake-on-propose) is a flag: it
registers the waker, checks-and-swaps an `AtomicBool`, and — like any
consumed-flag design — depends on the register-before-check ordering to
avoid losing a wake that lands between "check" and "park." Building
`animus-control::RaftNode::metadata_watch()` (ADR 0031 §trigger, a *caller*-
facing "has the applied index moved past what I last saw" notification
rather than a single internal consumer's wake), the natural shape is instead
an `AtomicU64` watermark: `changed(last_seen)`'s `poll` just checks
`current > last_seen` — true state, not a consumed edge — so a change that
already happened before the future was ever created or polled resolves
immediately on the very first poll, with no dependence on registration
timing at all. The register-before-check discipline is still followed (for
the case where the change happens *after* the first poll, before a
subsequent one), but the *design* no longer has a race to reason about for
the "already happened" case — it isn't consuming evidence that could be
consumed by nobody. General rule when building a wake primitive: if the
"did the awaited thing happen" question can be phrased as a comparison
against a monotonically increasing counter/index/version (not just "did an
edge fire"), prefer that framing — it is strictly more robust than a
one-shot flag and costs nothing extra (a `fetch_max` instead of a `swap`).
Keep the flag-consuming shape only when the event genuinely has no ordered
value to compare against (a bare "something happened, go do your own
re-check" nudge, which is what `ProposeSignal` actually needs — the
consensus loop doesn't care *how many* proposals queued, only that it should
wake up and drain). (`animus-control::node::MetadataWatch`.)
