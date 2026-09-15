# A "recompute the target fresh every tick" convergence loop is only as stable as its liveness-derived input — without debounce, ordinary failure-detector flap makes the target itself move faster than the mover can converge, a livelock one layer below the loop's own correct logic (issue #528, ADR 0062 §2/§3).

**A "recompute the target fresh every tick" convergence loop is only as
stable as its liveness-derived input — without debounce, ordinary
failure-detector flap makes the target itself move faster than the
mover can converge, a livelock one layer below the loop's own correct
logic (issue #528, ADR 0062 §2/§3).** `animus-control`'s directed-Placing
reconcile phase (`split_placing_reconcile`) recomputed `select_replicas`
fresh off current cluster membership on every tick, on the explicit
reasoning "never trust a persisted target, always recompute" — a
reasonable-sounding echo of the same crate's own ordinary repair/
rebalance phases, which really do need to re-derive their answer every
tick since *their* target is genuinely supposed to track live state.
Under sustained write load with real host contention, the ordinary
failure detector (ADR 0012) flapped cluster members `Active`↔`Down` at a
sub-second timescale — every flip changed which candidates
`select_replicas` saw, so it picked a **different** target essentially
every tick, each retarget restarting the data plane's own learner-phased
membership-change sequencing (`reconfigure_step`: add-learner → catch-up
→ promote → remove) from scratch. The target moved strictly faster than
the mover could ever complete one cycle — a downstream completion loop
(§3, `animusd::split_placing_completion`) that was itself entirely
correct, and its own convergence predicate, and its own settle-window
fix from an *earlier* incident (see this ADR's rung-6/rung-7 amendments)
all had nothing to observe, because the state they were watching for
never actually arrived. **The generalizable lesson**: auditing a
convergence loop's own logic for correctness is not the same audit as
checking whether the *value it converges toward* is itself stable
relative to the loop's own cadence — a loop whose target is re-derived
from a churny, liveness-based input (cluster membership, a failure
detector's verdict, anything with a "flip" as its unit of change) needs
either (a) a target that is computed once and then driven toward
verbatim until an explicit, deliberate, ideally *replicated* re-decision
happens, or (b) an explicit debounce/dwell on the input before it's
allowed to move the target at all — recomputing "fresh" on every
tick is the wrong default whenever the input can plausibly move faster
than the mover it feeds. The fix here: the target became authoritative
(driven toward verbatim while every member is healthy), a genuinely-dead
member pauses the drive rather than retargeting immediately, and only a
member continuously down past a dwell window triggers a recompute — and
that recompute is itself a REPLICATED command (`MetaCommand::
RetargetSplitPlacing`), not a locally-recomputed value fed straight into
the mover, so the new target is stable for every replica and every
subsequent tick rather than being re-derived (and potentially
re-flapped) independently by whichever node happens to be leader at the
time. See ADR 0062's 2026-09-01 amendment for the full incident,
investigation, and fix; `crates/animus-control/CLAUDE.md`'s ADR 0062
entry for the as-built mechanism.
