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

### Kubernetes (operator-managed)

Editing `spec.image` on an `AnimusCluster` (or any other pod-template field, including a
`controlNodes` growth) is a **gated rolling upgrade driven by the operator** (ADR 0073 Phase 3,
ADR 0060 "Upgrades"). There is no ungated window: the operator owns the StatefulSet's
`updateStrategy.rollingUpdate.partition`, applies a changed template together with
`partition = nodes`, then lowers it one ordinal at a time, only while every member is `Active`,
`GET /admin/roll-health` is `ok` everywhere and the previous pod is `Ready`, healthy and has
reported the new range. A control leader that is next gets a leadership transfer first. The
operator never deletes a pod (no new RBAC).

1. **Before.** Same preparation as the whole-cluster procedure: a verified backup or export
   (the first start of the new binary on a pod is its point of no return). The cluster must be
   healthy, have at least three nodes and a PodDisruptionBudget `maxUnavailable >= 1`
   (not one control node); smaller shapes are *not rolled* (the template is staged, nothing
   restarts, `UpgradeBlocked` says why: use the whole-cluster procedure).
2. **Edit** `spec.image` to the new release's image (one release step only; a skipped release is
   refused by the first upgraded pod's startup range check and the roll pauses there). Optionally
   set `spec.upgrade.finalize: Auto` (and `spec.upgrade.soakSeconds`) to let the operator finalize.
3. **Watch** `kubectl get animuscluster NAME -o yaml`: `status.upgrade` (`phase`, `onNew`/`total`,
   `activeClusterVersion`, `inFlightNode`) and the conditions `UpgradeInProgress`, `UpgradeBlocked`
   (names the reason: a D2 health reason, an unobservable admin port, a node stalled for 15
   minutes, a finalize blocker), `UpgradeFinalizePending`, `RollComplete`, `UpgradeChangesHeld`.
   `animus cluster roll status ADMIN` shows the same derived state from inside the cluster.
4. **Finalize.** With the default `finalize: Manual`, when every pod is on the new binary the
   operator sets `UpgradeFinalizePending`: run `animus cluster finalize <control-leader-admin-addr>`
   (irreversible). With `Auto` the operator finalizes once the roll is complete, `can_finalize`
   holds and the cluster has stayed healthy for `soakSeconds`; a failure (a `Down` member) is
   retried, never forced.
5. **If a pod never becomes healthy** the roll stops at that pod and the rest stay on the old
   binary (the cluster serves normally at the old cluster version). **Fix forward**: set
   `spec.image` to a fixed image and the gate re-targets it; or wipe that pod's PVC and let it
   rebuild from peers; or restore from backup. **Reverting `spec.image` to the pre-roll image is
   refused** (webhook) or pinned (reconciler, `UpgradeChangesHeld`) once any pod has run the new
   binary. `spec.nodes` / `spec.controlNodes` edits are held until `RollComplete`.
6. Not covered: `spec.storage.ephemeral` clusters (a restarted pod has lost its data by design).

The whole-cluster path for Kubernetes (recreate the `AnimusCluster`, mechanics untested) remains
the fallback for shapes the operator will not roll:

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

**Maturity of the operator path.** The operator's decisions are proven by fakes-level tests, the
shared `animus-roll` machine by the `sim_cluster_roll_orchestrator` corpus, and a real-process
previous-release roll by the `upgrade-previous-release` CI job. Kubernetes' own partition
semantics are exercised only by the nightly `kind` job (`upgrade-kind-nightly`, `E2E_UPGRADE=1`
in `scripts/e2e-kind.sh`), which has not yet had a verified run. **Known issue: do not roll
while multi-item transactions are in use** (issues #1237, #1238, found by the previous-release
test: an ungated transaction-envelope version can crash an N-1 replica, and acknowledged writes
were lost across a roll with transactions); use the whole-cluster procedure, or stop
transactional traffic, until they are fixed.

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

## ADR 0073 Phase 2/3 status: mixed-version and rolling upgrades

- **Mixed-version running (Phase 2):** a replicated cluster version, feature gates, the handshake
  range and Finalize; N-1 and N can run in one cluster, N-1 to N only. See the rolling procedure
  above. Two *unrelated* builds, or skipping a release, remain unsupported; a node whose range
  excludes the cluster version refuses to start by name.
- **Rolling upgrade (Phase 3, done 2026-10-05):** the per-node procedure with `animus cluster roll
  plan|wait|status`, `GET /admin/roll-health`, the `roll` status object, the dashboard Version
  card, and the operator-driven roll from a `spec.image` edit (above). The roll's core: restart one
  node at a time, wait until healthy, then `cluster finalize`; **there is no rollback once a node
  has run the new binary**.
- Criterion E-7 (rolling-upgrade procedure) is partially met by this page; the operator path's
  Kubernetes-semantics evidence is the nightly `kind` job, not yet run (see "Maturity" above).
- **Open (Phase 3 findings):** issues #1237 and #1238 (rolling with transactions), #1235
  (a `SimCluster` Memory-backend restart oddity, test-only). D4(b), a replicated maintenance mark
  that suppresses repair churn during a roll, is a pending maintainer decision now that
  the churn has been measured (ADR 0073's Phase 3 as-built amendment).

Distinguish from rotation restarts: restarting nodes one at a time on the **same** build
(certificate rotation, flag changes) is fine ([cert-rotation.md](cert-rotation.md)).

## Maturity

Whole-cluster compatibility is covered by the upgrade-restart harness and golden fixtures.
The stop/upgrade/start procedure itself, and the Kubernetes recreate path, were not executed
by the author on real nodes.
