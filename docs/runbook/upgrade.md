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

See the section "PENDING ADR 0073 Phase 2/3" below. Short version: **never run two
different builds in one cluster, even briefly.**

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

## Rollback

Restart-in-place on the old binary is refused by name if the new binary already ran
(and is not designed to work: ADR 0073 Option B, "no rollback once a node has run the new
binary"). To roll back: stand up a fresh cluster on the old version and restore from the
pre-upgrade backup/export ([backup-restore-pitr.md](backup-restore-pitr.md)); writes after
the upgrade are lost unless exported. Test this rehearsal before you need it.

## PENDING ADR 0073 Phase 2/3: mixed-version and rolling upgrades are not supported

- **Mixed-version running (Phase 2):** a replicated cluster version and feature gates, so
  N and N-1 can talk. Design accepted (ADR 0073 2026-10-03 amendment, rolling-installable over
  live Phase 1 binaries, N-1 to N only); the first parts (cluster version in `Metadata`,
  handshake extension, era-on refusal) have landed on `main` but are not wired into the
  running node (P2-C; ADR 0073 P2-A notes: "P2-A never starts an era in production"). Today the
  internal wire is version-tagged but **not negotiated**; two builds cannot run
  in one cluster.
- **Rolling upgrade (Phase 3):** a per-node roll runbook/CLI, a roll status view,
  and operator `spec.image` orchestration (OnDelete, one pod at a time). Not started. When
  it exists, this page gets a rolling procedure; ADR 0073 says its core is: restart one node
  at a time, wait until `Active` with no under-replicated tablet, then a `cluster finalize`
  step; and that **there is no rollback once a node has run the new binary**.
- Until then criterion E-7 (rolling-upgrade procedure) stays open by dependency.

Distinguish from rotation restarts: restarting nodes one at a time on the **same** build
(certificate rotation, flag changes) is fine ([cert-rotation.md](cert-rotation.md)).

## Maturity

Whole-cluster compatibility is covered by the upgrade-restart harness and golden fixtures.
The stop/upgrade/start procedure itself, and the Kubernetes recreate path, were not executed
by the author on real nodes.
