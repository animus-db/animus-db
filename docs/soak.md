# Soak

R-01 sub-track (a) ([`docs/roadmap.md`](roadmap.md),
[ADR 0074](adr/0074-production-readiness-exit-criteria.md), criteria A-1 to A-4
in [`production-readiness.md`](production-readiness.md)): **real `animusd`
processes** (bare multi-process on loopback, no root) under a **continuous
recorded DynamoDB-wire workload** for hours or days, with the `animus-test`
oracles run over the recorded history and per-node **resource-trend
assertions** (RSS, open fds, threads, disk, WAL bytes, SSTable file count,
queue gauges must not grow monotonically).

It reuses the chaos harness (`crates/animusd/tests/chaos_support/`: cluster
processes, the DynamoDB client, the list-append workload and recorder, the
oracle wiring; see [`chaos.md`](chaos.md)) with no faults armed. The workload is
the chaos history recorder, not `animus-bench`'s generator: the bench generator
measures latency and does not yield the append-only history that
`check_cycles`/`check_durability` need. It is the same key/op mix the oracles
were validated against, with a slower default pace. The operator-on-`kind` leg
is not built (see "Not done").

## Run it

```sh
export CARGO_TARGET_DIR=/path/to/target   # debug build is enough
# a 15 minute leg
ANIMUS_SOAK_DURATION=15m \
  cargo test -p animusd --features soak --test soak -- --nocapture
# the exit-criterion run, on dedicated hardware (7 consecutive days)
ANIMUS_SOAK_DURATION=7d ANIMUS_SOAK_SAMPLE_SECS=60 ANIMUS_SOAK_RESTART_EVERY=12 \
  ANIMUS_SOAK_DIR=/data/soak ANIMUS_SOAK_OUT=/data/soak-out \
  cargo test -p animusd --features soak --test soak --release -- --nocapture
```

The `soak` cargo feature keeps it out of the per-push gates (plain `cargo test`
never schedules it; `clippy --all-features` still lints it).

| Knob | Default | Effect |
|---|---|---|
| `ANIMUS_SOAK_DURATION` | `10m` | total length: seconds, or `s`/`m`/`h`/`d` suffix |
| `ANIMUS_SOAK_SEED` | name hash | seed of the workload ops and the port range; printed |
| `ANIMUS_SOAK_NODES` / `_TABLETS` | 3 / 4 | cluster size (>= 3), workload-table tablets |
| `ANIMUS_SOAK_EPOCH_SECS` | 300 | length of one verification epoch (see below) |
| `ANIMUS_SOAK_SAMPLE_SECS` | 15 | resource sampling interval (use 60 for multi-day) |
| `ANIMUS_SOAK_WARMUP` | duration/4, max 6h | samples before this are ignored by the trend check |
| `ANIMUS_SOAK_PACE_MS` | `20,60` | per-client pause between ops: `base,spread` ms |
| `ANIMUS_SOAK_RESTART_EVERY` | 0 | `kill -9` + restart one node (rotating) every N epochs |
| `ANIMUS_SOAK_DIR` / `_OUT` | temp dir | scratch data dir parent; artifact dir parent |

## What it checks

**Epochs keep the history bounded.** A 7-day history held in memory would grow
without limit and `check_cycles` would not finish. Instead the run is cut into
epochs. Each epoch uses a fresh key range, runs the workload, stops it, reads
every key back (consistent reads through two different nodes), runs
`check_durability`, `check_convergence`, `check_cycles` (with the converged
state fed back in as reads), the eventual-read-prefix and transaction-atomicity
checks over *that epoch's* history, then drops the history. The oracles' properties
are per key and each key belongs to one epoch, so nothing is lost by cutting.
Memory is bounded by one epoch's history, and the op trace is capped at 50k lines.

**Cold data.** At every epoch end the previous epoch's keys and epoch 0's keys
are re-read and must equal what was recorded when they were verified
(`[cold-data]`): data written hours ago survives compaction, restarts and
splits unchanged. Keys two epochs old are then deleted (epoch 0 is kept as the permanent canary), so the live data set
is bounded by design (and tombstone reclamation and compaction run
continuously); any resource growth that remains is a leak, not the workload.

**Process health.** Any node exiting on its own (`[node-exit]`), a `panicked at`
line in a node log (`[node-panic]`), a restarted node that never serves again
(`[availability]`), or fewer than 50 acknowledged writes in an epoch
(`[non-vacuity]`) fails the run.

**Resource trends.** Every `ANIMUS_SOAK_SAMPLE_SECS`, per node: `VmRSS` and
`Threads` from `/proc/<pid>/status`, open fds from `/proc/<pid>/fd`, bytes under
the node's data dir (total, WAL files, count of `sst-*` files: a compaction
that falls behind shows as SSTable-count and byte growth), and the gauges
`demux_queued_frames`, `demux_queued_bytes`, `spawned_task_handles_tracked`
from `/admin/metrics` (names from `crates/animus-env/src/metrics.rs`).
`animus_test::soak::evaluate` (pure, deterministic, unit-tested on growth,
plateau, sawtooth, step-then-flat, late-onset and slow-leak series) drops the
warm-up, cuts the rest into six equal windows, reduces each to its median, and
flags **growing** when the last window is a new high above every earlier
window by more than the tolerance, or when the series climbs steadily
(first to last window above tolerance, at least 80% of steps rising, and a
least-squares slope that agrees). Tolerance is
`max(abs, rel x first-window median)` per series (the `SERIES` table in
`tests/soak.rs`). A series with too few samples is `Insufficient`: printed, not a failure,
and the run is unproven for it, so size the duration so warm-up plus six
windows of at least three samples fit.

## Artifacts

`<out>/soak-<seed>/`: `samples.csv` (always: `secs,node,series,value`),
`trend-summary.txt`, `events.txt`; on failure also `history.json` (the failing
epoch's history, `animus_test::export` format), `op-trace.txt`, `violations.txt`
and each node's log.

## CI and the 7-day run

`.github/workflows/soak.yml` runs a short leg (default 45 minutes, up to 5h
via `workflow_dispatch`) weekly and on demand, uploading the artifacts on
failure. GitHub-hosted runners cap a job at 6h, so the 7-day exit run is a
documented command for dedicated hardware, not a CI job. It is not a required
check: a red run is a finding to triage, never to retry or quarantine.

## When it fails

Keep the artifact directory. A `[durability]`/`[cycles]`/`[cold-data]` violation
is a correctness finding: replay-minimize it against the sim corpora (ADR 0074
section 1). A `[resource-trend]` line names the node, series and rule; compare
`samples.csv` against the node log and `/admin` state before concluding leak vs.
a legitimate step. Do not widen a tolerance without a documented reason.

## Not done

The operator-on-`kind` leg; running faults concurrently with the soak (chaos
owns faults; `ANIMUS_SOAK_RESTART_EVERY` only adds periodic `kill -9`); the
7-day run itself; node log rotation (logs grow with the run; budget disk).
