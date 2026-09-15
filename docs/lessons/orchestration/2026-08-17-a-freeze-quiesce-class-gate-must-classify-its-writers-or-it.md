# A freeze/quiesce-class gate must classify its writers, or it deadlocks the drain-before-retire ordering it exists to enable

**A freeze/quiesce-class gate must classify its writers, or it deadlocks
the drain-before-retire ordering it exists to enable** (ADR 0050 Train B
rung 5, 2026-08-17). The split-cutover freeze first rejected *every*
write on the frozen parent — but the cutover's own vetoes wait for the
GSI drain and backfill seeder to finish consuming that parent, and
finishing requires those consumers to WRITE (cursor rows, footprints,
synthetic seed records). Result: a structural deadlock — the gate blocked
the very progress it was waiting on, caught red by the revived
split-during-backfill e2e. The general form: any "stop the world, let
consumers drain, then retire" sequence has two writer classes — user
data (the thing being frozen) and consumer bookkeeping (the thing that
measures drain progress) — and the gate is only sound if it blocks the
first class alone. Corollary found the same day: run NO consumer arms at
all on a not-yet-serving (`Building`) replica-to-be — its bookkeeping
rows can land in a *sibling's* scope (the token-truncated cursor-key
shape) and poison the sibling's own min-over-rows watermark.
