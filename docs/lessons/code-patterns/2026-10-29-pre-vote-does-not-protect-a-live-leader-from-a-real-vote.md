# Pre-vote does not protect a live leader from a real vote

Density corpus cell 3 failed on one seed: a freshly elected leader was deposed by a
node whose pre-vote it had granted a moment earlier, as a candidate (issue-less nightly
failure, seed `0x1c49137310331dab`, ADR 0009's 2026-10-29 amendment).

What generalises:

- **Pre-vote guards the pre-vote phase only.** Once a node holds a pre-vote majority it
  sends a real `RequestVote` at a higher term, and a grant can be stale by the time it is
  acted on: under CPU starvation the grant, the candidate's own vote responses and the
  eventual `RequestVote` all sit in queues for seconds. A leader that steps down on any
  higher term is therefore deposable by a stale grant. The lease check belongs on the
  real vote too (Raft thesis 4.2.3, etcd `inLease`), with leadership transfer as the one
  recognised bypass.
- **A lease alone did not make the oracle pass.** The higher-term node's reject of the
  leader's heartbeat deposes the leader anyway. The fix that removed the dip was at the
  source: a candidate that grants a pre-vote concedes its own candidacy, so it cannot win
  a term it has already waved a rival past.
- **Do not let a safety-flavoured predicate follow a starvation-sensitive signal.** Making
  the pre-vote refusal follow the check-quorum lease (so a quorum-less leader would grant)
  regressed a seed: acks queued in the leader's own CPU queue looked like a lost quorum.
  Keep check-quorum as lease validity for the real vote, not an active step-down, and
  keep the leader's unconditional pre-vote refusal.
- **A timeout stretch capped at a few base timeouts cannot cover tens of seconds of queue
  delay** (`max_lag_ms` reached 20-40 s in this cell). Any new time-window rule that reads
  "heard within an election timeout" must be checked in the starvation corpora
  (`density_admission_corpus`, `election_backpressure_corpus`), not only in pure tests.
- **Reproduce with the dip, not the cause.** The oracle counts a falling led count between
  samples, so confirm a fix by tracing the per-group (role, term) timeline at the failing
  step, not by checking the one message path you changed.
