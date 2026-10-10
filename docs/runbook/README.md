# AnimusDB operations runbook

Procedures for operating a self-hosted AnimusDB cluster. Pre-alpha. This runbook is roadmap item R-01 sub-track (e); the beta exit
criteria it serves (E-1 to E-9) are defined in `docs/production-readiness.md`
(introduced by the R-01 criteria PR).

**Every command, flag, endpoint and config key in these pages was checked
against the source on `main` (commit `e8c037da`, 2026-10-04).** Where a
procedure has no tool, the page says so, and describes the safe manual
procedure or says "unsupported". Each page ends with a **Maturity** line
saying what has been exercised by a test and what is derived from reading the
code and ADRs only. Nothing in this runbook has been executed against a real
multi-node cluster by its author; see [game-day.md](game-day.md) for the
outstanding drill that will change that.

## Pages

| Situation | Page |
|---|---|
| A node is unreachable, its scrape is absent, or it restarts | [node-down.md](node-down.md) |
| Replace a failed or retired node | [node-replace.md](node-replace.md) |
| Remove a healthy node permanently (scale down) | [node-decommission.md](node-decommission.md) |
| The control plane has lost quorum | [control-plane-quorum-loss.md](control-plane-quorum-loss.md) |
| A tablet (data-plane Raft group) is leaderless, under-replicated or lost quorum | [tablet-unavailable.md](tablet-unavailable.md) |
| Clients see `ProvisionedThroughputExceededException` or `ServiceUnavailable` | [overload-and-throttling.md](overload-and-throttling.md) |
| A node's disk is full or filling | [disk-full.md](disk-full.md) |
| Backup, restore and point-in-time-recovery drill | [backup-restore-pitr.md](backup-restore-pitr.md) |
| Rotate TLS certificates | [cert-rotation.md](cert-rotation.md) |
| Rotate the encryption-at-rest key | [encryption-key-rotation.md](encryption-key-rotation.md) |
| Upgrade AnimusDB | [upgrade.md](upgrade.md) |
| Size a cluster | [capacity-planning.md](capacity-planning.md) |
| Rehearse all of the above on `kind` | [game-day.md](game-day.md) |

### Alert entry points

Short pages that the shipped alert rules link to (each states what the alert means and
the first checks, then points to the full procedure):
[quorum-risk.md](quorum-risk.md), [control-plane-leader.md](control-plane-leader.md),
[tablet-leaderless.md](tablet-leaderless.md), [replica-health.md](replica-health.md),
[engine-recovery.md](engine-recovery.md), [txn-recovery.md](txn-recovery.md),
[errors-5xx.md](errors-5xx.md), [throttling.md](throttling.md), [auth.md](auth.md),
[network.md](network.md), [stream-backlog.md](stream-backlog.md),
[disk-space.md](disk-space.md), [cert-expiry.md](cert-expiry.md);
`node-down.md` is shared with the node alerts.

## The `animus admin` CLI is currently broken: use curl

On `main` at `e8c037da`, every `animus admin <sub>` (and `seed`, `decommission`,
`control-*`) fails with `animus: client handshake with <admin-addr> failed: TimedOut`.
`animus-cli`'s `http_call` goes through `maybe_tls_connect`, which always runs the
client-protocol preamble exchange before sending HTTP, and the admin listener does not
answer it. Reproduced against a running dev cluster; `animus status <client-addr>` and `curl`
against the admin port work. Pages show the `curl` form (HTTP/JSON, the same endpoints the
CLI wraps). Add `--cacert <ca.pem>` and `https://` when TLS is on. `jq` is used only for
readability.

## Conventions used in every page

**The `animus` CLI.** `animus` is the binary built from `animus-cli`
(`cargo build -p animus-cli`). Admin subcommands are
`animus admin <sub> <admin-addr> [args]`. With TLS on, put the global flag
first: `animus --tls-ca /path/ca.pem admin health <admin-addr>`.
Any CLI error (including `animus admin` with no subcommand) prints the full
usage text.

**`<admin-addr>`** is a node's admin listener (`host:port`; in an
operator-managed cluster every pod's admin port is `spec.basePort + 3`,
default `14003`; in a `gen-config` file it is each node's `admin` field).
`GET /admin/peers` on any node lists every node's admin address.
The admin port has **no authentication** (ADR 0020): bind it to a management
network only. Everything below that says "admin API" is this port.

**The control-plane leader.** `drain`, `remove`, `decommission`,
`control-add`, `control-remove`, `control-grow` and `control-transfer` are
served **only by the control-plane leader and are deliberately not relayed**.
Against a follower they fail with a "not the control-plane leader / retry on
the leader" error. Find the leader with:

```sh
animus admin raft <any-admin-addr>      # "leader": "<node-id>", "is_leader": bool
animus admin peers <any-admin-addr>     # node id -> admin address book
curl -s http://<any-admin-addr>/admin/status | jq '.node_addrs'   # id -> addresses
```

**Metrics.** `GET /metrics` on the **DynamoDB port** (not the admin port) is a
plain `name value` text export, unauthenticated even when SigV4 is enabled,
no `animus_` prefix (for example `control_is_leader`, `throttled_writes`).
`GET /admin/metrics` is the same data as JSON plus per-tablet `throttle`,
`request_rates` and `stream_change_rates`.

**Health endpoints (admin port).**

| Endpoint | Meaning |
|---|---|
| `GET /admin/live` | 200 whenever the admin server answers. Never gates on cluster state (Kubernetes liveness probe). |
| `GET /admin/health` | 200 only if this node has heard from a control-plane leader recently (3 election timeouts); 503 otherwise (Kubernetes readiness probe). It says nothing about tablets. |

**Logs.** `animusd` logs through `tracing` to stderr; set `RUST_LOG`
(default `info`). A background task that panics dies silently apart from one
`error` log line; see [disk-full.md](disk-full.md) and
[node-down.md](node-down.md) for why that matters.

**Always pass `--dir`.** Without it `animusd --config ... --node I` stores
data under `$TMPDIR/animusd-node-I` (and `join`/`control`/`data` similarly).
A production process must pass an explicit, persistent `--dir`
(the operator mounts `/var/lib/animus`).

**Timings that shape every procedure** (constants in
`crates/animus-control/src/node.rs`): heartbeat every 100 ms, a member is
marked `Down` after 500 ms of silence, and the control leader starts
re-planning that member's replicas away only after it has been `Down` for a
further 5 s (`REPAIR_DWELL`). A node that is down for more than roughly
5.5 s therefore starts losing its replicas to other nodes. There is no
maintenance mode.

## Known gaps this runbook cannot paper over

These are product gaps, found while writing the pages; they are listed so
nobody discovers them during an incident.

1. Control-plane quorum loss has an offline, data-losing recovery tool
   (`animusd recover-control`, ADR 0077) that is proven in simulation but not
   drilled on a real cluster; there is none yet for a tablet group
   ([control-plane-quorum-loss.md](control-plane-quorum-loss.md)).
2. Disk-full on a WAL write is handled (named 503 `StorageFull`, self-recovery) but
   proven in simulation only; an LSM-engine ENOSPC is still unhandled and may leave a
   live-but-wedged process ([disk-full.md](disk-full.md)).
3. No rolling or mixed-version upgrade
   ([upgrade.md](upgrade.md)); no encryption-key rotation
   ([encryption-key-rotation.md](encryption-key-rotation.md)); no
   hot-reload of TLS material ([cert-rotation.md](cert-rotation.md)).
4. No connection cap or admission control beyond per-table throttling
   (pending R-01(d); [overload-and-throttling.md](overload-and-throttling.md)).
5. No published capacity numbers (pending B-01/C-17;
   [capacity-planning.md](capacity-planning.md)).
