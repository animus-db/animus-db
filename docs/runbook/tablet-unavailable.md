# A tablet is unavailable, under-replicated or at risk

A tablet is one Raft group (RF = `min(nodes, 3)` replicas) holding a slice of one
table's hash ring. Each has a single leader serving linearizable reads and
writes; eventually-consistent reads (`ConsistentRead: false`, the wire default)
can be served by any replica. Conventions are in [README.md](README.md).

## Symptoms

Clients: `ServiceUnavailable` (503) or `InternalServerError` (500) for some
keys only; `ConsistentRead: true` reads failing while `false` reads succeed.
Alerts: [tablet-leaderless.md](tablet-leaderless.md), [replica-health.md](replica-health.md).

## 1. Find the tablet and its state

The dashboard (`GET /` on any admin port) derives a per-tablet status that
means "is the data at risk":

| Status | Meaning (from `dashboard_core.js`) |
|---|---|
| `quorum-lost` | fewer than a majority of the tablet's **assigned** replicas are on nodes not marked `Down`. It cannot commit; one more failure loses data. Overview shows `critical`. |
| `under-replicated` | some assigned replica's node is `Down`; redundancy is reduced, repair pending. `degraded`. |
| `forming` | all assigned nodes alive, but no leader yet or fewer groups hosted than configured: a transition (new table, split child, catch-up). Not a risk by itself. |
| `healthy` | a leader exists and every configured replica is hosted. |

Note this status is inferred from membership (`Down` is a 500 ms heartbeat
timeout) and from the groups each node reports; it is not Raft's own opinion.

Without the dashboard:

```sh
curl -s http://<admin>/admin/status | jq '.members, .tablets'    # replicas per tablet, member status
curl -s http://<each-admin>/admin/raftkv                         # per hosted group: tablet, role, leader, term, voters, learners,
                                                                  #   commit_index, engine_applied_index, quiesced, refused_as_voter
curl -s http://<each-admin>/admin/txns                           # transaction tracker per group
```

`/admin/raftkv` is per node; query every node that should host the tablet.
A quiesced group (`quiesced: true`, idle for 5 s by default) is normal: it wakes on
the next write or message and admin reads never wake it.

## 2. Decide

| Finding | Action |
|---|---|
| A replica's node is `Down`, majority alive | wait or fix the node ([node-down.md](node-down.md)). Repair re-plans after 5 s `Down` if there is a spare `Active` node. |
| `forming` for minutes with all nodes alive | check each hosting node's `/admin/raftkv` for the group; `refused_as_voter` true means a wiped replica ([replica-health.md](replica-health.md)); otherwise a stuck host reconcile: restart the node that does not host it. |
| `quorum-lost`, the missing nodes can return | bring them back with data ([node-down.md](node-down.md)). The group recovers by itself; no data is lost if a majority of the *last committed* replicas returns. |
| `quorum-lost`, a majority is permanently gone | **no tool recovers it.** `POST /admin/raftkv/reconfigure` needs the group's leader (a majority); there is no force or unsafe variant. Restore the table from a backup or PITR into a new table ([backup-restore-pitr.md](backup-restore-pitr.md)) and repoint clients; data written after the last backup/PITR point is lost. |
| Leader exists but writes still fail | check disk on the leader node ([disk-full.md](disk-full.md)); `cp_proposals_rejected_not_leader` and `cp_read_barriers_timed_out` on `GET /metrics`. |
| Hot tablet (throttling) | [overload-and-throttling.md](overload-and-throttling.md); `POST /admin/tablet/split {"tablet":<id>,"split_key":"<key>"}` (CLI `split`) splits it. |

Manual placement is rarely needed: `POST /admin/raftkv/reconfigure
{"tablet":<id>,"voters":["n1","n2","n3"]}` (leader only, one single-server step
per call, add-before-remove and catch-up gated; it answers
`stepped_to` or "already at target"). The control leader does the same on its own
for repair and rebalance.

## 3. Confirm

Dashboard Overview back to `healthy`; `/admin/raftkv` shows a `leader` and equal
`commit_index`/`engine_applied_index` across replicas; clients succeed; the
alerts clear.

## Maturity

Endpoints and the status ladder were checked against `admin.rs` and
`dashboard_core.js`; `/admin/raftkv`, `/admin/status`, `/admin/health` output
was inspected on a local dev cluster. Majority loss of a data tablet is covered
by the simulation corpora (restart, partition, leader kill) but this procedure was not
drilled on a real cluster.
