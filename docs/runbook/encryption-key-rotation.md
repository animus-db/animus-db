# Encryption-at-rest key rotation

Mechanism: ADR 0069. Conventions are in [README.md](README.md).

## Verdict: in-place key rotation is NOT supported

There is no re-encryption mechanism and no key versioning. ADR 0069 states key
rotation is out of scope for v1. Do not try to rotate by editing the key file:
every node validates a marker file in each encrypted directory
(`.animus_encryption_marker`, one per data directory and one per segment/backup store
directory) at startup and refuses to start with a different key, with a named error.
The refusal table (verified in ADR 0069 and its tests):

| Directory has marker | Key given | Result |
|---|---|---|
| yes | no | refuse: encrypted directory, no key |
| yes | yes, authenticates | proceed |
| yes | yes, does not authenticate | refuse: wrong key |
| no, other files exist | yes | refuse: key against a plaintext directory |

The key file is 64 hex characters (32 bytes), for example `openssl rand -hex 32 > key.hex`,
passed with `--encryption-key PATH` (per node) or the per-node config field
`encryption_key_path`. On Kubernetes it is a pre-existing `Secret` named by
`spec.encryptionKeySecretName` with data key `key`, mounted read-only on every pod. The
operator never generates or inspects key material. **Changing the Secret's content
under the same name rolls no pod and would, on the next restart, put a node on a key that
does not match its directory.**

## Documentation contradiction you need to know about

ADR 0069's decision section said rotation means "standing up a fresh,
differently-keyed replica and letting Raft catch it up, then decommissioning the old one".
That text is now struck through and superseded by ADR 0069's 2026-10-10 amendment (which
also records the keyring plan for real rotation). It was written for the per-node disk key. The later "As-built: cluster store" amendment
(2026-09-07) put the default replicated segment and backup stores under **the same
cluster-wide key** (nodes exchange ciphertext; a node with a different key cannot serve or
accept those objects). So a differently-keyed replacement node is not a supported
mix: do not rely on the replica-replacement path. The cluster-wide stores also hold
all existing streams segments and backups sealed under the old key.

## What you can do

1. **Key lost or suspected compromised, stolen disk, no live compromise.** A stolen disk
   or backup is protected by the old key as long as the key itself did not leak. If the
   key leaked, the only complete remedy is a new cluster with a new key (below).
2. **Migrate to a new key = build a new cluster and copy the data.**
   - Stand up a new cluster with a new key ([upgrade.md](upgrade.md) conventions for
     versions; a fresh `--dir` on every node, the new key file on every node, same TLS).
   - Move data with `ExportTableToPointInTime` on the old cluster to a customer S3 bucket
     (plain DynamoDB JSON, not sealed by AnimusDB; protect the bucket) and `ImportTable`
     on the new one ([backup-restore-pitr.md](backup-restore-pitr.md)). Import creates
     tables from a minimal key definition; recreate GSIs, TTL, streams, throughput
     and credentials yourself. Or copy at the application level.
   - Old backups (`--backup-store`) are sealed under the **old** key and cannot be read
     by the new cluster. Keep the old key safe for as long as you keep those backups,
     or delete them after the migration.
   - This procedure is a design inference from ADR 0068/0069; it has not been tried.
3. **Do not** start nodes with mixed keys against shared stores; each node's own marker
   check refuses a mismatch (and a non-matching node never joins the replica set).

## Provisioning rules that prevent the problem

- Provision the identical key file on **every** node before the first start; keep an offline
  copy in a secrets manager. Losing the key loses the data on those disks and in
  those stores, with no recovery.
- Permissions: the key file must be readable by the `animus` user (the operator mounts it
  mode 0444 on a restricted volume).
- Record which key id/name each cluster uses; the marker does not carry one.

## Maturity

Derived from ADR 0069 and the operator guide; the refusal behaviour is covered by
`animusd/tests/encryption_at_rest*_e2e.rs` and sim corpora. No migration was attempted.
Recommendation to the maintainers: a key-versioning design (ADR) is a beta prerequisite for
any deployment with a key-rotation policy.
