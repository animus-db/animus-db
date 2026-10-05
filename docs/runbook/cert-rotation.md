# TLS certificate rotation

Mechanism: ADR 0064. Conventions are in [README.md](README.md). Alert entry point:
[cert-expiry.md](cert-expiry.md).

## What is and is not supported

- **No hot reload.** `TlsConfig::load()` reads the three PEM files (`cert_path`,
  `key_path`, `ca_path`) once, when the listeners are bound. There is no file watcher,
  signal or admin call that reloads them. A rotated file on disk has no effect until the
  process restarts (ADR 0064 Decision 6; re-verified in the 2026-09 amendment).
  **Rotation = replace the files, then restart every node.**
- **A rolling restart is fine here.** Restarting nodes one at a time is not a version
  upgrade; all nodes run the same binary. (Contrast [upgrade.md](upgrade.md).)
- Which ports: mutual TLS on `internal` and `intra` (every node presents a cert signed
  by the cluster CA and verifies its peer against it); server-only on `client`,
  `dynamo`, `admin`, `console`. A cluster is all-TLS or all-plain.
- The CA file may contain several certificates; each is trusted
  (`root_cert_store` loads every cert in the file), which makes CA rotation possible
  (below). This overlap behaviour is read from the code, not exercised by any test.
- Every certificate must carry SANs for every name peers dial it by (IP and, on
  Kubernetes, the pod DNS name). The operator's cert-manager `Certificate` uses wildcard
  SANs under the headless service so scaling never reissues it.

## A. Leaf certificate rotation (CA unchanged)

1. Issue the new leaf cert/key from the **same CA**, valid well before the old one
   expires. Never reuse a bare self-signed certificate as both leaf and CA across
   nodes: a reissue then replaces the trust anchor too (ADR 0064, 2026-09 amendment);
   use a CA-backed issuer so `ca.crt` stays byte-identical.
2. Put the new files where each node reads them.
   - Bare metal: overwrite the files at the paths in each node's `tls` config section
     (`cert_path`, `key_path`, `ca_path`) or `--tls-cert/--tls-key/--tls-ca`.
   - Kubernetes with `spec.tls.secretName`: update the `Secret` (`tls.crt`, `tls.key`,
     `ca.crt`); the kubelet refreshes the mounted files after about a minute.
   - Kubernetes with `spec.tls.certManager`: cert-manager renews into
     `<name>-tls` before expiry on its own.
3. **Restart every node, one at a time** (below). On Kubernetes the operator does
   **not** restart pods for a `Secret` content change: the config-hash covers only
   whether TLS is configured, not the files.
4. Verify each node serves the new certificate (below).

Rolling restart procedure (both platforms): for each node in turn (control voters
last, keeping a control majority up at all times):

```sh
# Kubernetes (highest ordinal first; delete is not an eviction, so the PDB does not apply)
kubectl -n <ns> delete pod <name>-<ordinal>
kubectl -n <ns> wait --for=condition=Ready pod/<name>-<ordinal> --timeout=300s
# bare metal: stop (SIGTERM, graceful) and start the process with the same --dir and flags
```

Then **wait for convergence before the next node**: `/admin/health` 200 on the
restarted node, `members.<id>.status == "Active"` in `GET /admin/status`, and no tablet
`under-replicated` on the dashboard ([tablet-unavailable.md](tablet-unavailable.md)).
Readiness alone is not enough: it only says the node has heard a control leader,
not that its tablet replicas have caught up, and restarting the next node
too soon can leave a tablet without a caught-up majority (writes stall until it
catches up; no acknowledged write is lost).

Expect data movement: a node absent for more than about 5.5 s has its replicas
re-planned onto other nodes (see [node-down.md](node-down.md)). A pod restart
usually exceeds that. This is safe but costs I/O; there is no maintenance mode to
suppress it. `kubectl rollout restart statefulset/<name>` would also work but the
operator server-side-applies the StatefulSet; this runbook did not verify it keeps the
restart annotation, so the explicit pod loop is the documented path.

Verify the served certificate (use the node's DynamoDB or admin address):

```sh
openssl s_client -connect <host:port> -servername <host> </dev/null 2>/dev/null | openssl x509 -noout -subject -enddate
```

Also check `net_handshake_refused` and `client_handshake_refused` on `GET /metrics`
stay flat ([network.md](network.md)).

## B. CA rotation (overlap procedure; not exercised)

Only the trust anchor changes; do it in three rolling restarts so peers always
share a trusted CA:

1. Write a CA bundle file containing **old and new** CA certificates as `ca_path`
   on every node; rolling restart. Now every node trusts both.
2. Issue leaves from the new CA, install them as `cert_path`/`key_path`; rolling
   restart. Peers present new-CA leaves and verify them via the bundle.
3. Remove the old CA from the bundle; rolling restart.

CLI clients: `animus --tls-ca <ca.pem> ...` must trust the new CA too
(`--tls-ca` before the subcommand).

## Failure modes

- A node that cannot read or parse its PEM files fails at startup with a named error
  (nothing is half-applied); restore the previous files and restart.
- Mixed old/new certificates are fine as long as both chain to a CA every peer
  trusts. Mixed trust anchors with no overlap fail every handshake in both directions
  (the exact incident ADR 0064's amendment records for a scale-up).
- Expired certificate on a running node: handshakes fail from the expiry instant on
  every TLS port. Alert on expiry
  ([cert-expiry.md](cert-expiry.md)) and rotate ahead of it.

## Maturity

No hot reload exists, and the file-read-at-bind behaviour and operator non-restart on
`Secret` change are from the code and ADR 0064. The procedure was not executed on a real
cluster; the CA-overlap procedure depends on multi-certificate CA files, read from
`root_cert_store` but not tested end to end.
