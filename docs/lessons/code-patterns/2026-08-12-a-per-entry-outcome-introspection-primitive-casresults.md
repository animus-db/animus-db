# A per-entry outcome introspection primitive (`CasResults`-shaped: `BTreeMap<log_index, Outcome>`, read back after a coarse "did this index apply" check) is not implied by that coarse check — a snapshot install can satisfy the coarse check without ever populating the fine-grained one for the entry in question.

**A per-entry outcome introspection primitive (`CasResults`-shaped:
`BTreeMap<log_index, Outcome>`, read back after a coarse "did this index
apply" check) is not implied by that coarse check — a snapshot install
can satisfy the coarse check without ever populating the fine-grained
one for the entry in question.** Adding `StageOutcome` (ADR 0018 §2's
apply-time write-key conditions amendment, `animus-cp-data`) alongside
the pre-existing `wait_applied(index) -> bool` (confirms only
`engine_applied_index() >= index`) initially paired them as "wait, then
unconditionally fetch and `.expect()` the outcome" — reasoning that
"applied" and "has a recorded outcome" were the same fact, true for
every *other* command in this crate. They are not, for any command whose
outcome is recorded by the *individual* apply arm rather than derived
from the post-apply engine state: a replica catching up via
**`InstallSnapshot`** (after losing leadership) advances
`engine_applied` in one jump from the received image, without
individually re-running the apply arm for every entry the image already
covers — so an entry whose outcome only that per-entry arm would have
recorded can have `engine_applied_index() >= index` true while its
outcome map entry is simply absent. `ANIMUS_TXN_SEEDS=5` over the
multi-tablet corpus (`animus-test/tests/txn_serializable.rs`) hit this
as a hard, deterministic panic — not a hang, not a wrong answer, a
crash, because the code trusted a fact ("wait_applied implies outcome
recorded") that happened to hold for every pre-existing caller of the
wait-then-fetch pattern (`Cas`'s own `compare_and_swap` was never
vulnerable, because it was never split into two steps — it polls
`cas_result` *directly* in its wait loop from the start). **The
generalizable check**: whenever a "wait for index N to apply" helper and
a "look up what happened at index N" store are two separate calls,
verify explicitly that nothing in the codebase can advance the wait
condition without also having populated the store for that exact index
— a snapshot/compaction/batch-glob path is the recurring way this
invariant quietly breaks, since those paths exist specifically to
advance a coarse watermark without replaying fine-grained per-entry
work. The fix is to poll the fine-grained store directly (one loop, no
separate coarse check), exactly like the one pre-existing caller that
was never at risk already did — never a hard `.expect()` on a fact
that's true "usually" rather than by construction.
