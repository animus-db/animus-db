# Fail readiness on a consensus-loop panic, not on every spawned-task panic

**`/admin/health` fails on `consensus_task_panics`, not on all spawned-task
panics** (issue #1220). A panic in a never-restarted loop (control Raft driver,
`Metadata` apply loop, a tablet group's driver or apply loop) leaves the node
silently dead for that group, and only a restart repairs it, so it must pull the
node from rotation. Every other spawned task (per-request handlers, reapers,
sweepers) loses at most one unit of work, so those only bump the exported
`spawned_task_panics` counter and a warning alert. The seam is
`Spawner::spawn_critical` (default: plain `spawn`); any new `Spawner` wrapper
(`EncryptedEnv` did) must forward it, or the flag is silently lost and health
stays green. The real-task proof must be a `ProdEnv` target: `SimEnv` cannot
observe a panic.
