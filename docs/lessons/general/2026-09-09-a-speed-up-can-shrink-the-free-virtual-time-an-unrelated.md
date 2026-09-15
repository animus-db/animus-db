# A speed-up can shrink the "free" virtual time an unrelated check used to accidentally rely on — a corpus non-vacuity check that raced an in-flight replica migration (2026-09-09, #772 PR 2/2)

Fixing the executor-cost regression above (`dynamo_fast`, quiescence) made
`sim_cluster_dynamo_corpus.rs`'s `dynamowire_forward_heavy` cell fail its
own `non_hosting_ok_writes > 0` assertion — deterministically, at a fixed
seed, on both a `--test-threads=1` re-run and with quiescence toggled
fully off, ruling out cross-test-thread interference and quiescence's own
timing as the cause before looking anywhere else. Direct instrumentation
(printing `final_replicas`/`ok_writes_by_node` at the point of the check)
found the real mechanism: this cell's own randomized workload can draw a
tokened `TransactWriteItems`, which auto-provisions the internal
`__animus_txn_idempotency` table and, via ADR 0029's own add-then-remove
migration sequence, can leave the corpus's modeled tablet's `live_
replicas()` snapshot showing one MORE member than its configured
replication factor for as long as that migration is still converging — an
old replica not yet released alongside the new one already added. Reading
`live_replicas()` while that transient window is still open narrows the
"non-hosting" node set from 2-of-4 down to 1-of-4, and for one specific
seed's own random draw sequence, the SOLE remaining non-hosting node had
already recorded a successful write that this narrowed, mid-migration
snapshot then silently excluded — a measurement race in the CORPUS'S OWN
checking methodology, not a forwarding defect in the system under test.

**Why the speed-up exposed a pre-existing race rather than one it
introduced.** The old, slow `SimCluster::dynamo` unconditionally burned a
full `OP_BUDGET` (12s) of virtual time on every call regardless of how
quickly the op itself resolved — including the several `TransactGetItems`
calls `force_resolve_all_keys` makes between the workload finishing and
this check running. That incidental, unplanned "grace period" (tens of
seconds of virtual time nobody asked for) happened to be enough for the
migration to settle before the ORIGINAL 25 seeds' own checks ever ran, so
the race was always there but never observed. `dynamo_fast` removed that
grace period as a side effect of removing the waste it existed to fix —
the check's own dependency on "enough incidental time elapses before I'm
read" had been invisible until the thing supplying that time for free was
fixed.

**First attempt — moving the same live snapshot to run BEFORE
`force_resolve_all_keys`/the probes instead of after — did not fix it.**
The migration was already complete (or already past the vulnerable window,
just at a different point) by the end of the workload's own `DRAIN`
period in some runs and still transiently over-counted in others,
depending on the seed; moving the read earlier only moved which seed hit
it, the same symptom the initial quiescence/`dynamo_fast` toggle
experiments already demonstrated (turning one knob shifts which of 200
seeds gets unlucky, never proves the knob is the cause or the cure on its
own). The general form: when a flaky-seeming assertion's true dependency
is "some background process must have settled by the time I'm read," a
one-shot read at any FIXED point in the timeline is still one-shot —
fixed correctly by turning it into the SAME converged-or-timeout poll this
file's own durability/convergence checks already use a few lines below,
waiting for the replica count to genuinely return to the tablet's own
configured value before trusting the snapshot.

**The general lesson**: a performance fix that makes something finish
faster can retroactively remove slack an entirely unrelated correctness
check was implicitly, invisibly leaning on — "isolate the two toggles and
re-test with each held at its old value" (quiescence off, `--test-
threads=1`) is what proved this wasn't the fix's own new bug before
chasing the real mechanism down; and a live snapshot of any state that can
be mid-transition needs the SAME converged-or-timeout discipline this
crate already applies to every other eventual property, not just the ones
that already had it.
