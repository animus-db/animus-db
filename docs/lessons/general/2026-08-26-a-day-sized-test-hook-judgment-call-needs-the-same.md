# A day-sized test-hook judgment call needs the SAME reasoning both times it's made, and a growing list of shapes it doesn't yet cover (issue #298 residuals, seal-boundary regression)

Two consecutive rounds of the same investigation independently reached the
same conclusion — a deterministic `ProdEnv` regression for the seal-boundary
overlap race (`seal_now`'s `metadata_fresh()` fix) needs new test-hook
plumbing (a `#[cfg(test)]` pause point mirroring `dynamo::
rmw285_confirm_gate`'s precedent, but on `animus-control`'s `DRIVER_APPLIED`
metadata-apply cache-refresh timing rather than a lock-scope boundary) and
judged it larger than a bounded pass both times. That repeated, independent
agreement is itself useful signal — worth recording explicitly rather than
re-deriving from scratch a third time — but it is not a substitute for
actually building the hook: **`animusd` has no `animus-sim` dependency, so
none of its race conditions are `SimEnv`-reachable**, which means every one
of them stays soak-detected-only (real timing, real flake rate) until
someone budgets the dedicated pass to build the missing test-hook seam. A
fifth, still-unconfirmed duplicate-delivery shape surfaced in this same
round's bounded 20-run soak (a within-one-already-sealed-shard duplicate
for one member of a transacted write pair) — extensive code-first
re-verification of `TxnResolve`'s per-key apply idempotency, its always-
fresh-`ts`-minting, and `trim_split_child`'s boundary math did not reach a
confirmed mechanism, so no speculative fix was attempted (per this repo's
own standing rule: fix only what's confirmed). The generalizable point:
when a bounded-round investigation defers the SAME piece of groundwork
twice in a row for the SAME stated reason, that is the signal to schedule
it as its own dedicated task rather than let a third round re-litigate the
same feasibility judgment — the growing list of shapes it would let get
regression-tested (now: the seal-boundary overlap AND this fifth shape) is
the accumulating cost of not doing so.
