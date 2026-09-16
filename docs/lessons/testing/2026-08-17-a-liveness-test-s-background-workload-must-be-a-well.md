# A liveness test's *background workload* must be a well-behaved Raft client too, whenever its convergence assert demands every command commit — fire-and-forget proposing is only acceptable when the assert doesn't count the commands.

**A liveness test's *background workload* must be a well-behaved Raft
client too, whenever its convergence assert demands every command commit —
fire-and-forget proposing is only acceptable when the assert doesn't count
the commands.** The sustained-churn `ProdEnv` liveness test dripped 300
`propose(..)` calls with the result discarded, then asserted all 300
members converged; one mid-churn leadership transition (explicitly within
the test's own `MAX_TRANSITIONS` tolerance!) silently dropped the ~34
commands proposed across it — `NotLeader` returns were never retried, and
appended-but-superseded entries have no propose-time signal at all — so
all three nodes sat flat at 266/300 for the entire 20s budget (issue
#269). The flat-count shape is the diagnostic: slow I/O shows *progress*
across the poll; identical, motionless counts mean the commands were never
committed, and no budget bump can fix that. The fix is the two-sided
client discipline the production entries above already prescribe, applied
to the workload loop: retry `NotLeader` against the re-resolved leader
(never-accepted, retry is free), and confirm commits against the acting
leader's *applied* state, re-proposing what it lacks (accepted-unconfirmed
— safe here because the command is idempotent). Note the tension this
resolved: a test that *tolerates* N leadership transitions while asserting
exact convergence is inconsistent unless its proposer survives those
transitions. (`animus-control/tests/prod_liveness.rs::
sustained_metadata_churn_over_a_real_engine_stays_live`.)
