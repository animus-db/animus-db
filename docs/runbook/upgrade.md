# Upgrading AnimusDB

Policy and mechanism: ADR 0073 (upgrade compatibility), ADR 0060 ("Upgrades"). Conventions
are in [README.md](README.md).

## What is supported (ADR 0073 Phase 1, done 2026-10-03)

**A whole-cluster stop, upgrade and restart across any post-baseline versions** (baseline
commit `9a9f972f`, 2026-09-29): a newer binary reads everything an older
post-baseline binary wrote (WALs, SSTables, manifests, snapshots, backups, PITR segments,
export objects); this is enforced by per-version decoders, golden fixtures that are never
edited, and the upgrade-restart harness (`animus-test` tiers 0/1, `animusd`
`sim_cluster_upgrade_corpus` tier 2: per-push and nightly). A **downgrade is refused by
name** (an unsupported-version error at startup); rollback is restore from backup.

## What is NOT supported

See the section "PENDING ADR 0073 Phase 2/3" below. Short version: **mixed N-1/N running is
supported only through the rolling procedure below (era on, one release step); never run two
unrelated builds in one cluster, and never skip a release.**

## Whole-cluster procedure

Plan a maintenance window; clients cannot be served during it.

1. **Before.**
   - Read the release notes for format or flag changes. There is no changelog or
     version endpoint yet (workspace version is `0.0.0`, no `--version` flag): identify
     builds by image digest or binary checksum, and record the exact old and new ones.
   - Healthy cluster: `/admin/health` 200 everywhere, every member `Active`, no tablet
     `under-replicated` ([tablet-unavailable.md](tablet-unavailable.md)), no firing alerts.
   - **Take and verify a rollback copy with the old version**: an on-demand backup of
     every table and/or an S3 export ([backup-restore-pitr.md](backup-restore-pitr.md)),
     plus a disk snapshot of each data volume if your platform offers it. Rollback needs
     this: the new binary may rewrite local files in a format the old one refuses.
   - Stop client traffic (or accept errors).
2. **Stop every node**, gracefully (SIGTERM or SIGINT; `animusd` shuts down its groups
   cleanly). Bare metal: stop all units. Wait until all are down.
3. **Install the new binary or image** on every node. Keep each node's `--dir`, key
   and TLS files and flags unchanged.
4. **Start every node** (all of them, with the new build, close together). The control
   plane needs a majority of voters to elect; data nodes mirror metadata from it.
5. **Verify**: `/admin/health` 200 everywhere, members `Active`, no `refused_as_voter`
   or startup errors in logs, dashboard Overview `healthy`, a read and write round trip
   through the DynamoDB port, row/item counts on key tables against pre-upgrade numbers,
   `GET /metrics` handshake-refused counters flat ([network.md](network.md)).

A node that refuses to start with a named version error is running an older
binary than the data on its disk: start the newer binary on it.

### Kubernetes (operator-managed): honest limits

The operator does not orchestrate upgrades. It does **not reject a `spec.image` edit**, and
the StatefulSet controller then rolls the pods one by one, which is exactly the
unsupported mixed-version window. **Do not edit `spec.image` on a running cluster.**
Documented ADR 0060 alternative: recreate the `AnimusCluster`. The mechanics are
not tested; the reasoning is:

- Pods' volumes are StatefulSet claim templates (`data-<name>-<ordinal>`), and no
  retention policy is set, so deleting the StatefulSet leaves the PVCs.
- Never lower `spec.nodes` to stop pods: the operator treats it as a scale-down and
  drains and removes members ([node-decommission.md](node-decommission.md)).
- Candidate sequence, to be proven on `kind` ([game-day.md](game-day.md)): quiesce
  clients; `kubectl delete animuscluster <name>` (children are garbage-collected, PVCs
  stay); confirm every pod is gone; `kubectl apply` the same manifest with the new
  `spec.image` and the same name, namespace and `spec.nodes`/`controlNodes`/`storage`
  so the pods reattach to their existing PVCs; wait for `phase: Ready`; verify as above.
  Do not change anything else in the spec in the same step.
  Recreate keeps identity because ids derive from the name and ordinal.

## Rolling upgrade, node by node (manual; ADR 0073 Phase 2 supported, Phase 3 P3-A tooling)

A cluster whose cluster-version era is on (Phase 2) runs mixed N-1 and N builds safely,
so you can restart nodes one at a time with no client outage. **N-1 to N only**: never
skip a release. A cluster still on Phase 1 binaries has no era, so the first roll onto a
Phase 2 build has no version observation; use the health gate below and nothing else.
The orchestration (a CLI `animus cluster roll` and operator-driven `spec.image` rolls)
is later Phase 3 work; until then you drive the loop below by hand with `curl` against
each node's admin port (`ADMIN` below is a node's admin address).

**Read this before step 1: a node's first start of the new binary is its point of no
return.** From that moment it writes the new on-disk formats. There is no rollback for
that node (ADR 0073 Option B, fix forward): the only way back is the restore procedure
under "Rollback". Take and verify the backup/export first, exactly as for the
whole-cluster procedure.

Never use `drain` to roll a node. `drain` is the decommission path: it moves every
replica off the node, leaves the member `Leaving` (which blocks Finalize) and the node
must re-register ([node-decommission.md](node-decommission.md)). A roll is a restart in
place.

1. **Plan.** `curl -s http://ADMIN/admin/cluster-version`:
   - `era_active` must be `true` and `active` is the version you are on;
   - `roll.phase` is `not_started`; `roll.remaining` is the order to follow: data-only
     nodes first, then control voters, **the control leader last**;
   - `roll.down` should be empty; bring any `Down` member back (on the new binary) or
     decommission it first, because Finalize refuses while one exists.
2. **Gate.** `curl -s http://ADMIN/admin/roll-health` must say `"ok": true`. If not,
   `reasons[]` names why (see the table below). Do not touch the next node until it is
   `ok`.
3. **If the node is the control leader** (`curl -s http://NODE_ADMIN/admin/health` shows
   `"is_control_leader": true`), move leadership first:
   `curl -s -X POST http://NODE_ADMIN/admin/control/transfer -d '{"to":"<another voter>"}'`
   and wait until `control_leader_recent` is true on another node. Data-plane (tablet)
   leaders re-elect on their own; clients see brief retries.
4. **Restart the node on the new binary or image** (`systemctl restart`, a new container
   image, ...). Keep `--dir`, keys, TLS files and flags unchanged. Send SIGTERM and let it
   shut down cleanly.
5. **Wait for it to be healthy.** Poll **the restarted node's own** admin port
   (`/admin/roll-health` has a node-local clause, so asking another node is not enough)
   until `"ok": true` *and* `/admin/cluster-version` shows that node reporting the new
   range (its row's `range.max` is now the new version). The node flips `Down` to
   `Active` within seconds, long before its groups catch up: `local.caught_up_groups`
   equal to `local.hosted_groups` is what stops you opening the gate too early.
6. **Repeat** 2-5 for every remaining node, **including `Down` ones**.
7. **Finalize** when `roll.phase` is `ready_to_finalize` (`can_finalize` is `true`):
   `animus cluster finalize <admin-addr>`, or `curl -s -X POST
   http://LEADER_ADMIN/admin/cluster-version/finalize`. Finalize is manual, irreversible,
   and one version step at a time. Until you finalize, the cluster still behaves as the
   old version (a soak window), even though each upgraded node already writes new files.

The dashboard Overview shows the same state in a Version card (cluster version, `N of M`
nodes on the new build, roll phase, what is next, blockers and the roll-health verdict,
with a Finalize button once ready).

### `GET /admin/roll-health`

Read-only, never a readiness probe. `ok` is true only when the control group has a
recent leader (and, when the answering node can see it, a quorum of reachable voters),
the answering node has synced `Metadata`, every member is `Active`, no tablet is
`quorum-lost` or `under-replicated`, no group on the answering node has a learner mid
catch-up, and every group on the answering node knows a leader and has its engine within
16 entries of its commit index. `reasons[].kind` is one of: `no_control_leader`,
`control_quorum_lost`, `metadata_not_synced`, `member_not_active` (`node`),
`tablet_quorum_lost` / `tablet_under_replicated` (`tablet`), `learner_pending` and
`local_group_not_caught_up` (`tablet`). `tablets.forming` is reported but does not fail
`ok`. The tablet verdicts use the same ladder as the dashboard
([tablet-unavailable.md](tablet-unavailable.md)).

### What a roll costs

A restart that outlasts the repair dwell (5 s of continuous `Down`) lets placement repair
start rebuilding the node's replicas on any cluster with a spare node, so a slow restart
can cause avoidable rebuild traffic (correct, only wasteful). On a cluster with no spare
candidate (RF 3 on 3 nodes) nothing is rebuilt. Restart promptly and do not stop a node
for longer than needed. A maintenance mark that suppresses this is a possible follow-up
(ADR 0073 Phase 3, D4); it is not built.

## Rollback

Restart-in-place on the old binary is refused by name if the new binary already ran
(and is not designed to work: ADR 0073 Option B, "no rollback once a node has run the new
binary"). To roll back: stand up a fresh cluster on the old version and restore from the
pre-upgrade backup/export ([backup-restore-pitr.md](backup-restore-pitr.md)); writes after
the upgrade are lost unless exported. Test this rehearsal before you need it.

## PENDING ADR 0073 Phase 2/3: mixed-version and rolling upgrades are not supported

- **Mixed-version running (Phase 2):** landed (a replicated cluster version, feature gates,
  the handshake range, Finalize); N-1 and N can run in one cluster, N-1 to N only. See the
  rolling procedure above. Two *unrelated* builds, or skipping a release, remain
  unsupported; a node whose range excludes the cluster version refuses to start by name.
- **Rolling upgrade (Phase 3):** the manual per-node procedure, `GET /admin/roll-health`,
  the `roll` status object and the dashboard Version card are documented above (P3-A). The
  `animus cluster roll` CLI and operator `spec.image` orchestration are not built yet; the
  operator still does **not** gate a pod roll (see "Kubernetes" above: do not edit
  `spec.image` on a running cluster). The roll's core: restart one node at a time, wait
  until healthy, then `cluster finalize`; **there is no rollback once a node has run the new
  binary**.
- Criterion E-7 (rolling-upgrade procedure) stays open until the CLI and operator pieces
  land and the manual procedure has been exercised on real nodes.

Distinguish from rotation restarts: restarting nodes one at a time on the **same** build
(certificate rotation, flag changes) is fine ([cert-rotation.md](cert-rotation.md)).

## Maturity

Whole-cluster compatibility is covered by the upgrade-restart harness and golden fixtures.
The stop/upgrade/start procedure itself, and the Kubernetes recreate path, were not executed
by the author on real nodes.
