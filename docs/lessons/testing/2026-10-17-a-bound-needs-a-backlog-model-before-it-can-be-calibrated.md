# A propose bound can only be calibrated against a model in which a backlog forms (ADR 0080)

The first overload corpus used a slow-fsync model and either never built a backlog
(the bound never fired) or livelocked elections (an 80 ms disk starved the heartbeats, so
goodput measured the election storm, not the bound).

- **Make the apply path the bottleneck, not the disk**: a test engine that charges virtual
  time per applied op (`ApplyEngine`) grows `last_log_index - engine_applied` the way a
  slow LSM does, without touching consensus timing.
- **Measure the unbounded capacity in the same rig and assert a fraction of it**
  ("at least 80 % of the capacity with the bound off"), instead of a hard-coded rate that
  moves with the machine or the engine cost.
- **Every oracle needs a control that fails it** (bound off, bound ignoring class, no
  cold-wake refusal); a bound whose control also passes is not measuring anything.
- A sim test that needs an unresolved future must be driven with `spawn_task` plus stepping
  the simulator, never `block_on` (it hangs forever on a sim-only wakeup).
