# A fix that changes what an entry *does* needs a new variant, not a branch in apply

**Context.** #1233 closed a real divergence by making a txn decision on an
already-sealed range a no-op in the existing `TxnCommit`/`TxnAbort` apply arms. It
was correct in isolation and wrong for a mixed-version cluster: an old replica and a
new replica applying the *same committed entry* then disagree, and no later
mechanism reconciles them. R-01 review caught it before any release carried it.

**Lesson.**

- A bug fix in an apply arm is a **replicated-behaviour change**, the same class as a
  new command. "Apply stays a pure function of the entry and the state machine" is
  necessary, not sufficient: it must also be a pure function of the entry across
  *binary versions*.
- Apply cannot consult the cluster version (it cannot read it, and a restarting
  replica re-applies history under whatever it sees now). The repo's rule (ADR 0073
  decision 4) is the only sound shape: **keep the old arm byte-for-byte, add a new
  variant carrying the new behaviour, gate its emission at the proposer** (`Gate`,
  `GatedCommand`, the one `gated_propose` choke point), pick the variant in one
  helper (`txn_commit_cmd`/`txn_abort_cmd`). The legacy arm is never edited again.
- Say the cost out loud: the fix protects a cluster only after the finalize. Every
  test/sim/chaos harness that wants the fix must open the gate first (a fresh
  cluster starts at version 1), or it silently stops testing it.
- A fixture or a unit test that holds the gate at both values is cheap (`gates.rs`
  row test, `HostedOptions { features }` at version 3 and 4) and catches an arm that
  drifted back to a version-blind check. A mixed-version cell with a mutation
  (ignore the gate at the proposer) proves the proposer really consults it.
