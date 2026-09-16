# A "resume optimization, never the source of truth" status condition silently became the only trigger once its sibling signal stopped changing

Found closing out issue #864 (`animus-operator`'s S-07d `spec.controlNodes`
growth stall), after two earlier fixes for the same issue (a retry/logging
hardening, then a durable-storage fix) had already landed and a real
occurrence still stalled with the *original* signature.

## The trap: two independent "is there anything to do" signals, one of which stops moving

`reconcile`'s own growth call site read `if target_control_nodes > prior
|| already_growing { call advance_control_growth }`. `prior` comes from a
live-ish read (the applied `ConfigMap`'s own value); `already_growing`
comes from a status condition set by the *previous* reconcile. The design
doc for that condition says explicitly: it is "a resume optimization —
skip the live check once nothing is pending — never the source of truth."
That framing is correct as long as `prior` keeps changing to signal
"something is happening." It does not, here: by this crate's own design,
the applied `ConfigMap` jumps to the *full target* on the very first
reconcile after the spec edit (a real, load-bearing requirement — the
newly-promoted ordinal needs its final role in the config immediately, or
it can never restart into it at all). Once `prior == target`, the
`target_control_nodes > prior` half of the `||` can never be true again
for the rest of this growth — leaving `already_growing`, the condition
the design doc calls an optimization, as the *sole* remaining path to
ever call the function again. A comment that is true when written ("this
is just an optimization, alongside a signal that keeps moving") silently
stops being true the moment its sibling signal stops moving, with no
compiler or test forcing a re-check of that claim.

The condition itself is delivered from a `kube-runtime` watch-fed
reflector, not a live `GET` — ordinarily fast, but with no bound this
code's author could point to. A single missed or delayed round trip, at
any point during an active growth, was enough to stop the whole mechanism
permanently, since nothing else could ever make `target_control_nodes >
prior` true again.

## The generalizable rule

When an `OR` condition has two branches and one of them is documented as
"just an optimization" or "just a fast path," check what happens once the
*other* branch becomes permanently false (or permanently true) for the
rest of the scenario the optimization is supposed to matter in. If the
"real" branch can only ever fire once per event, the "optimization"
branch has quietly become the only thing keeping the mechanism alive for
every subsequent check — and inherits every reliability property the
optimization branch never had to have on its own (it doesn't need to be
correct instantly; it needs to be correct *eventually*, but "eventually"
across N reconciles 30 seconds apart is a much stronger requirement than
"eventually" within one). The fix here was to stop needing the
optimization to be a gate at all: call the expensive-looking function
unconditionally and let its own cheap internal live check (one fast
request in the steady state) decide whether there is real work — turning
an intermittently-fragile cross-reconcile handoff into a per-reconcile
fact nothing else has to keep alive. Prefer this shape whenever the
"expensive" work being gated is not actually expensive in the common
case: the guard meant to save a `GET` cost more, in outage risk, than the
`GET` itself.

## Why this took three fixes, not one, to find

Each of the three real occurrences behind issue #864 had a genuinely
different mechanism (a diagnosability/latency gap in a 409 retry; an
ephemeral-storage/wiped-voter quorum loss; this gating bug) — a
`SimEnv`/`SimCluster` reproduction of the growth logic itself converged
cleanly for all of them, because none of the three lived in the pure
consensus logic a simulator drives. Fixing the first plausible cause a
real failure's evidence pointed to is necessary but not sufficient when a
symptom ("growth stalls") can have more than one real, independent root
cause; each fix here was validated against a fresh, independently-
diagnosed real-cluster occurrence before being trusted, rather than
assumed to be *the* fix from code review alone.

See ADR 0060's 2026-09-15 amendment (Part C) and issue #864 for the full
investigation and the regression test
(`reconcile_still_attempts_growth_when_the_growing_condition_did_not_
survive`, `crates/animus-operator/src/controller.rs`).
