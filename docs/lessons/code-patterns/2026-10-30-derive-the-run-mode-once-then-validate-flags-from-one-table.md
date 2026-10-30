# Derive the run mode once, validate flags from one table

**What happened (issue #1422).** `animusd`'s `run()` had eight copy-pasted guards,
each rechecking `config_path.is_none()` before the real mode-conflict check, so
`--cluster 3 --cluster-control 1 --cores 4` reported the `--cores` error instead
of the conflict. Separately, the split dev path (`--cluster-control/--cluster-data`)
silently ignored a dozen flags it never threaded through (labels, throttles,
stores, S3, stream-seal).

**The fix.** Derive a `Mode` enum once, then check (a) conflicts, (b) flags vs mode
from one `FLAG_RULES` table (flags, allowed modes), (c) file loads. Record every
flag spelling in the parse loop so the table needs no per-flag variable.

**Why.** Per-flag guards encode the mode implicitly and in the wrong order; and
"documented gap, silently ignored" for a flag the operator believes is active is
the worse failure. To find which flags a path really honours, read the callee's
parameter list (`start_split_cluster_with_growth` takes only dir/ip/backend/
auto-split/sweep/auth/quiesce/heartbeat/shared-wal), not the prose. A pure
`(args) -> Result` test helper pins the error order without starting a node.
