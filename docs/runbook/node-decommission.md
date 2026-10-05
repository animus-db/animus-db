# Decommission (scale down) a healthy node

Permanently remove a working node: ADR 0032 drain, then remove. For a dead node
use [node-replace.md](node-replace.md). Conventions are in [README.md](README.md).

> The `animus admin ...` subcommands currently fail against the admin port
> (`client handshake ... TimedOut`; see [node-replace.md](node-replace.md)).
> Use the `curl` forms. All of them were run by the author against a local
> dev cluster.

## Preconditions

- Remaining nodes can hold the data: free space on the survivors, and at least
  as many `Active` nodes as the replication factor you want to keep
  (RF = `min(nodes, 3)`). A tablet cannot be re-homed without a candidate.
- All remaining nodes `Active` and no tablet `under-replicated`
  ([tablet-unavailable.md](tablet-unavailable.md)). Do not decommission during
  [quorum-risk.md](quorum-risk.md).
- If the node is a control voter, see "Control voter" below.
- Run drain/remove against the **control-plane leader**'s admin address; they are not
  relayed. A follower answers `this node is not the control-plane leader; retry on <addr>`.

## Procedure

```sh
L=<leader-admin-addr>; N=<node-id>
curl -s -X POST http://$L/admin/drain  -d "{\"node\":\"$N\"}"            # status "Leaving"
curl -s "http://$L/admin/member/drain-status?node=$N"                      # repeat until tablets_remaining == 0
curl -s -X POST http://$L/admin/member/remove -d "{\"node\":\"$N\"}"     # ok == proposal accepted
curl -s "http://$L/admin/member/drain-status?node=$N"                      # "absent" once committed
```

Then **stop the process**; removal is not a fence, and a restarted process
re-registers as a fresh join. CLI equivalent (when working):
`animus admin decommission <leader-admin> <node-id>`, which drains, polls
and removes, then prints that it is safe to stop the process.

`remove` refuses with a named reason: the node is a live control voter, is
still `Active`/`Joining`, or is still referenced by N tablets.
Draining moves replicas with the ordinary placement machinery (one move at a
time, add-before-remove, catch-up gated), so duration scales with data volume;
watch `GET /admin/raftkv` on the receiving nodes or the dashboard Placement
tab.

## Control voter

A combined node that is a control voter is refused until it is removed from the
control group (`GET /admin/control/members`, `voters`):

```sh
curl -s -X POST http://$L/admin/control/member/remove -d "{\"node\":\"$N\"}"   # add "force":true only after reading control-plane-quorum-loss.md
```

(CLI: `decommission <leader> <id> --force-control-remove` runs this step, with an
automatic leadership transfer if the target leads, and then the drain flow; it
never implies `--force`.) The guard refuses a removal that would leave fewer
than a majority of the *remaining* voters reachable and names the apparently
dead voters. Do not bypass it with `force` unless you have verified by hand that
every remaining voter is up. A removal that leaves exactly one voter succeeds with
a warning (no fault tolerance).

## Kubernetes (operator-managed): lowering `spec.nodes`

```sh
kubectl -n <ns> patch animuscluster <name> --type merge -p '{"spec":{"nodes":<N-1>}}'
kubectl -n <ns> get animuscluster <name> -o jsonpath='{.status.conditions}'
```

Documented operator behaviour (ADR 0060, `drain_and_remove_node`): highest ordinal
first, one pod fully removed before the next; for each it posts `/admin/drain`,
polls `drain-status` every 5 s up to 120 times (10 minutes), posts `/admin/member/remove`,
and only then lets the StatefulSet drop the pod and PVC. On failure it sets the
`DrainFailed` condition and keeps the replica count at the last drained pod.
`spec.nodes` below `spec.controlNodes` is refused
(`ScaleBelowControlNodesRefused`); `spec.controlNodes` cannot be lowered.

> **Suspected defect, not fixed here.** The operator posts `/admin/drain` to the
> admin port of the pod being removed. `/admin/drain` is control-leader-only
> and not relayed, and a data-only pod has no control handle at all.
> Reproduced against a running dev cluster: `POST /admin/drain` to a data-only
> node's admin address returns `409 {"error":"this node is not the
> control-plane leader; retry on <leader>"}`. If the same happens in a real
> cluster, an operator scale-down fails with `DrainFailed` for any data-only ordinal.
> The operator's scale-down is only tested against a fake admin client, and
> `scripts/e2e-kind.sh` never scales down. Until this is confirmed or fixed on
> `kind` ([game-day.md](game-day.md)), treat operator scale-down as unverified.
> Safe manual path: drain and remove the highest ordinal yourself against the
> control leader (the curl procedure above), and only then lower `spec.nodes`;
> because the operator will still try to drain the (already removed) pod
> first, the same 409 may still block that last step. If it does, file the
> defect; there is no supported workaround.

## Maturity

Drain, drain-status, remove, id reuse and the control-voter refusals are covered by
`animusd/tests/decommission.rs` and `control_membership*.rs`. The curl procedure was
run by the author on a local dev cluster. The Kubernetes scale-down is unit-tested
only against a fake admin client and has not been exercised on `kind`.
