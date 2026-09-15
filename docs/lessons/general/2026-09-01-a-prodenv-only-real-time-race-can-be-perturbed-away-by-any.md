# A `ProdEnv`-only real-time race can be perturbed away by any unrelated timing change — a fast converging run right after adding diagnostics proves nothing (issues #532/#537)

The learner-stall reproduction above is a genuine real-time race: it
converges reliably in seconds under `SimEnv` (no CPU-time cost model) and
under *any* incidental change to real scheduling on the `ProdEnv` host —
a background admin poller, a tracing subscriber initialized before
bring-up, even the act of adding an `eprintln!` diagnostic to the loop
under investigation. Every one of those perturbations shifts relative
timing just enough to let the lagging peer's round complete before the
next overlapping resend piles more work on top of it — the exact
mechanism the fix closes, momentarily avoided by luck instead of by
structure. **The trap this creates**: after instrumenting a suspected
`ProdEnv` livelock, the very next run frequently "just works" — and that
run proves *nothing* about whether the underlying defect is fixed, only
that this particular perturbed timing happened not to trigger it this
time. The only honest validation for this class of bug is the
**unmodified** reproduction, re-run **after** the diagnostics are removed
(or on a pristine checkout of the fix), ideally several times to see the
pre-fix failure rate before touching anything — see this same investigation's
own ADR 0009 amendment for a case where even a real fix, validated this
way, still left a residual, unexplained pass rate on one host: don't let
one lucky green run — pre-fix, mid-investigation, or post-fix — stand in
for a repeated, unmodified measurement.
