# A `SimEnv`-driven "batching OFF" control needs to inject its metrics handle the same way the "batching ON" cell does — reaching for the co-hosted-with-batcher shape for the unbatched proof gives zero traffic everywhere, not a compile error (ADR 0044 phase 2, C-02 PR 3 cutover).

**A `SimEnv`-driven "batching OFF" control needs to inject its metrics
handle the same way the "batching ON" cell does — reaching for the
co-hosted-with-batcher shape for the unbatched proof gives zero traffic
everywhere, not a compile error (ADR 0044 phase 2, C-02 PR 3 cutover).**
`RaftKvNode::start_hosted_with_batcher(env, .., None)` (the batcher-off
variant) still calls `env.metrics()` internally exactly like `start_
hosted` always has — under `SimEnv` that's the shared no-op handle
(`animus_env::Env::metrics`'s own default) unless the caller injects a
recording one, and there is no "co-hosted on shared node ids AND
injectable metrics AND no batcher" constructor to reach for. The first
draft of `heartbeat_batch_corpus.rs`'s new explicit opt-out cell copied
cell (a)'s co-hosted/fixed-leader shape verbatim, just passing `None`
for the batcher — it compiled clean, ran in well under a second, and
reported `low=0 high=0 ratio=NaN` (every counter genuinely zero, not a
logic bug in the assertion). The fix was going back to PR 1's own
independent-`Simulator`-per-group shape (`RaftKvNode::start_with_
metrics`, which forces `PRIMARY_STREAM` but *does* take an explicit
handle) for the unbatched case specifically — batching has nothing to
amortize across independent worlds anyway, so losing the co-hosted shape
costs nothing there. **General form**: when a `SimEnv` fixture family has
two "record traffic" constructors — one co-hosted-with-injectable-
metrics-but-only-via-a-companion-mechanism (here, the batcher), one
independent-worlds-with-injectable-metrics-directly — swapping a
parameter from `Some` to `None` on the first can silently strip the
metrics injection along with the feature being turned off, since the
handle was piggybacking on the very mechanism now disabled. A `low=0
high=0` (or any all-zero) result from a `SimEnv` metrics assertion is a
fixture-wiring bug to suspect immediately, not a "maybe nothing
happened" — a genuinely idle group over a multi-second window still
ticks its Raft heartbeat and produces nonzero traffic; zero-across-the-
board means the recording path itself never wired up.
