# A finite workload cannot witness "writes resumed after the fault"

Issue #1333: `disk_full_mid_5_s07` failed `acked_writes_after_heal > 0` on
pristine `main` at `ANIMUS_DISK_FULL_SEEDS>=8`. The cluster was fine: a probe
write was acked 1.6 s after space returned (one capped recovery-probe backoff).
The Elle workload (16 rounds per client) had simply drained before the fault
or the heal, leaving one in-flight read, so no Elle *append* could be acked
after the heal. A cell that passes at seeds 0-6 only because the workload
happens to outlive the window is a vacuous oracle waiting for a seed.

Rules:

- Assert liveness/resumption with an actor that cannot run out of work (the
  `spawn_window_probe` writer, stopped only after the post-heal drain), never
  with the count of a finite workload's completions after the fault.
- When a seed knob deepens a corpus and a new seed fails, read the history
  (invoke/ok times relative to fault and heal) before touching the system or
  the timeouts: whether the cluster was ever asked to write after the heal is
  the first question.
- Pin the failing seed as an always-on test (the knob only reaches deep seeds
  nightly): `raftkv_disk_full_pinned_seed_workload_drains_before_heal`.
