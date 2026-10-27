# Sample the applied index first and the recheck-before-giving-up becomes redundant

Follow-up to issue #1308's single-sample rule (`animusd::kind_poll`). `poll_probe`, `cp_put_local` and `cp_delete_local` kept a second "close the probe-vs-apply race" recheck (re-read outcome, value, applied index) before reporting superseded. It was only needed because they read the applied index AFTER the value probe.

Why one sample suffices: the apply task records an entry's outcome and merges its effects (`flush_pending`, or `install_engine_image` for a snapshot) BEFORE `engine_applied.fetch_max` publishes the index (`animus-cp-data` apply pass). So anything read after a sample of the index is at least as new as the sample, and an entry at or below the sampled index is final: its outcome is recorded (or aged out of the bounded map), its effects are readable, and a value that does not read back stays that way for this entry. Order matters: sample, then outcome, then value, then decide; never decide from a fresher index than the one that classified the outcome.

Gotcha for the next reader: a recheck that "can only help" can hide that the primary read was in the wrong order. Fix the order, then delete the recheck. The proof is a mutant that never returns superseded: the superseded-hang tests in `write_path` stop terminating.
