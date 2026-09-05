# ADR 0066 — SigV4 hardening: replicated credentials, rotation, and per-key allow lists

- **Status:** Accepted
- **Date:** 2026-09-05
- **Origin:** `docs/roadmap.md`'s S-02 ("SigV4 hardening")
- **Amends:** [ADR 0057](0057-sigv4-client-auth.md) (replaces its static-map
  credential store and its "not IAM" non-goals around rotation, replication,
  and per-key scoping — see that ADR's Amendment section)
- **Depends on:** [ADR 0057](0057-sigv4-client-auth.md) (the SigV4 verifier
  this ADR reuses unchanged), [ADR 0020](0020-admin-interface.md)
  (admin port trust posture, invoked for secrets traveling to
  `/admin/credentials`), [ADR 0064](0064-tls-on-every-port.md) (TLS on every
  port, including admin), [ADR 0038](0038-control-metadata-system-keyspace.md)
  (`Metadata` is driver-applied and durably mirrored per node — the
  mechanism this ADR's catalog rides), [ADR 0051](0051-dynamodb-ttl.md)
  (`env.wall_now()`, the calendar-time seam the rotation grace window uses),
  [ADR 0013](0013-replicated-schemas.md) (the replicated schema catalog this
  ADR's credential catalog sits beside in `Metadata`), [ADR 0059](
  0059-backup-restore.md) (the backup catalog row shape this ADR's
  `CredentialRow` is modelled on)

## Context

ADR 0057 shipped SigV4 signature verification against a **static credential
map read once from node config** (`dynamo_auth` / `--dynamo-auth PATH`),
deliberately scoped to "proof the caller holds a configured secret" and
explicitly **not** an authorization engine. Its own Non-goals section named
exactly what it deferred: rotation, replication of credentials through
`Metadata`, any dynamic credential API, and multi-tenancy or per-key table
scoping. Those are this ADR's subject.

The static map has four concrete costs once a cluster runs for any length of
time:

1. **No rotation.** Changing a secret means editing every node's config file
   (or the seeded JSON) and restarting — there is no way to introduce a new
   secret, cut clients over, and retire the old one without a window where
   either the old or the new secret is rejected somewhere in the cluster.
2. **No replication.** A data-only node (ADR 0035) mirrors `Metadata` but
   reads `dynamo_auth` from its own local config — two nodes can carry
   different credential sets by operator error, with no consistency check.
3. **No dynamic API.** Adding or removing a key requires a config change and
   a restart on every node that serves the client port, not a request.
4. **No per-key authorization.** Every valid key can do everything to every
   table — there is no way to hand one caller a read-only key, or scope a
   key to a subset of tables, short of running separate clusters.

`docs/roadmap.md` §6 ("Deliberately not planned") stakes out the boundary
this ADR must not cross: **IAM-the-service — policy language, condition
keys, temporary/STS credentials, per-item/attribute authorization, tenant
namespaces — stays out of scope.** This ADR adds a *replicated credential
catalog with rotation and a minimal per-key table/operation allow list*, not
an authorization engine. The distinction that matters: a policy here is one
flat `(tables, ops)` pair per key, evaluated once per request, with no
composition, no conditions, and no wildcard beyond "all tables" — a coarser
mechanism than IAM's, sized for the gap S-02 names and no further.

Three facts already settled elsewhere shape the design:

- **ADR 0038**: `Metadata` is `DRIVER_APPLIED` — a per-node async apply task
  owns it and durably mirrors it into a per-node system-keyspace
  `StorageEngine`. A credential catalog living in `Metadata` is therefore
  visible to every node (data-only nodes included, via their `Metadata`
  mirror) with no round trip to verify — the same reasoning ADR 0013's
  schema catalog and ADR 0059's backup catalog already rely on.
- **ADR 0051**: `env.wall_now()` is the seam for interpreting an
  externally-supplied absolute timestamp, deliberately never used for
  timing (every deadline/timeout/election keeps using `env.now()`, which
  cannot step backwards). A rotation grace window is exactly the shape
  `wall_now()` exists for: a calendar-time deadline supplied by the
  proposer and read back later, not a protocol timer.
- **ADR 0059**'s `BackupRow`/`MetaCommand` pattern (catalog keyed by an id,
  never by a mutable name; deterministic commands carrying their own
  timestamps; idempotence on identical payloads) is the template this ADR's
  `CredentialRow`/`MetaCommand` variants copy directly rather than
  reinventing a replicated-row shape.

## Decision

### 1. A replicated credential catalog in `Metadata`

`Metadata` gains `credentials: BTreeMap<AccessKeyId, CredentialRow>`,
replicated by the control plane exactly like the schema catalog (ADR 0013)
and the backup catalog (ADR 0059): every node — data-only nodes included,
via their `Metadata` mirror (ADR 0038) — verifies locally, with no round
trip to a control-plane leader on the read path.

```rust
pub struct CredentialRow {
    pub secret: SecretKey,
    pub previous: Option<PreviousSecret>,
    pub policy: Policy,
    pub enabled: bool,
    pub created_at: u64, // epoch seconds, supplied by the proposer
    pub updated_at: u64, // epoch seconds, supplied by the proposer
}

pub struct PreviousSecret {
    pub secret: SecretKey,
    pub valid_until: u64, // epoch seconds
}

pub struct Policy {
    pub tables: TableMatch,
    pub ops: BTreeSet<OpClass>,
}

pub enum TableMatch {
    All,
    Names(BTreeSet<String>),
    Prefixes(BTreeSet<String>),
}

pub enum OpClass {
    Read,
    Write,
    Ddl,
    Streams,
    Backup,
    Admin,
}
```

`created_at`/`updated_at` are epoch seconds **supplied by the proposer and
never read from a clock during apply** — the identical "timestamp travels in
the command payload" discipline every other `Metadata` mutation with a
timestamp already follows, since apply must stay deterministic across
replicas (ADR 0054's reasoning, restated for this catalog: apply reads
nothing but the command and prior state).

**Operation-class mapping** (every DynamoDB operation maps to exactly one
class; table-less operations need no table match):

| Class | Operations |
|---|---|
| `Read` | `GetItem`, `BatchGetItem`, `Query`, `Scan`, `TransactGetItems`, `DescribeTable`, `DescribeTimeToLive`, `ListTagsOfResource`, and other `Describe*`/`List*` calls scoped to a table |
| `Write` | `PutItem`, `UpdateItem`, `DeleteItem`, `BatchWriteItem`, `TransactWriteItems` |
| `Ddl` | `CreateTable`, `UpdateTable`, `DeleteTable`, `UpdateTimeToLive`, `TagResource`, `UntagResource`, `UpdateContinuousBackups` |
| `Streams` | `DescribeStream`, `GetShardIterator`, `GetRecords`, `ListStreams` |
| `Backup` | `CreateBackup`, `DescribeBackup`, `ListBackups`, `DeleteBackup`, `RestoreTableFromBackup`, `RestoreTableToPointInTime`, `DescribeContinuousBackups` |
| *(no table, always allowed to any enabled key)* | `ListTables`, `DescribeLimits`, `DescribeEndpoints` |

A key created with **no explicit policy gets `Policy { tables: All, ops:
{Read, Write, Ddl, Streams, Backup} }`** (every class except `Admin`, which
no wire operation needs — see Decision 6) — the pre-S-02 behaviour exactly,
so an existing key that never set a policy changes nothing about what it can
do.

### 2. New `MetaCommand`s

Modelled directly on `BackupRow`/its `MetaCommand`s (`meta.rs:326-421`,
`:1154` on), deterministic in apply, timestamps carried in the payload:

- `PutCredential { id: AccessKeyId, secret: SecretKey, policy: Policy,
  enabled: bool, now: u64 }` — create the row, or replace it if `id` already
  exists. Replacing the secret through `Put` is an **immediate cutover, no
  grace window** — `Put` is the create/redefine primitive, `Rotate` (below)
  is the only path that preserves the outgoing secret. A `Put` that changes
  only `policy`/`enabled` leaves `secret`/`previous` untouched.
- `RotateCredential { id: AccessKeyId, new_secret: SecretKey, grace_secs:
  u64, now: u64 }` — moves the row's current `secret` into `previous` with
  `valid_until = now + grace_secs`, installs `new_secret` as the current
  secret. A second `Rotate` inside an already-running grace window
  **replaces** `previous` outright (does not chain a third secret) — so at
  most two secrets are ever valid for a given `id` at once, mirroring how
  AWS IAM itself only ever allows two active access keys per user. No-op
  (other than the timestamp/`previous` bookkeeping above) if `id` does not
  exist is **not** accepted — rotating a nonexistent id is an error, unlike
  `Put`'s create-or-replace shape.
- `RevokeCredential { id: AccessKeyId }` — removes the row outright. An
  in-flight request already past verification is unaffected (verification
  already happened); the next request bearing that key id fails from
  `UnrecognizedClientException` onward. Idempotent: revoking an id that is
  already absent is a no-op, not an error — the same idempotence-on-repeat
  discipline every other `MetaCommand` in this catalog follows for a
  replayed/retried proposal.

Idempotence: an identical `Put`/`Rotate` payload replayed (retry after an
unconfirmed accept, ADR 0003/`ProposeResult::Accepted` semantics) applies to
the same resulting state — no compounding, no double-rotation from a
retried `Rotate` with the same `new_secret`/`now`/`grace_secs`.

All three join **every gating match site** the root `CLAUDE.md`'s standing
rule calls out for a new replicated/forwarded command variant:
`is_relayable_command` (a credential mutation must be relayable from a
follower-connected node exactly like `SetTableTtl`/`TagResource`/`SetTable-
Throughput` already are), the `mirror.rs` apply buckets, and any admin
command filter. Each gets a pinned regression test through a
follower-connected node (Decision 10), per the lesson that a missed
allowlist entry is a bimodal per-process flake the compiler cannot catch.

### 3. Verification and the grace window

The verifier the request hits first is **ADR 0057's unchanged, pure
`sigv4::verify`** — this ADR does not touch canonicalization, the HMAC
chain, or clock-skew checking. What changes is which secret(s) the caller in
`animusd` tries:

1. Look up `id` in `Metadata::credentials`. If absent, fall through to the
   static bootstrap map (Decision 4); if still absent, `Unrecognized-
   ClientException`.
2. If the row exists but `enabled` is `false`, treat it as absent —
   `UnrecognizedClientException` (a disabled key is not distinguishable
   from a nonexistent one on the wire, matching AWS's own behaviour of
   never confirming whether a key id ever existed).
3. Try `sigv4::verify` against `secret`. On success, resolve the policy
   (Decision 5) and proceed.
4. On failure, and only if `previous.is_some()` **and** `env.wall_now() <
   previous.valid_until`, try `sigv4::verify` against `previous.secret`. On
   success, increment `Metric::AuthRotatedSecretUsed` (Decision 9) so an
   operator can see stragglers before the grace window closes, then resolve
   the policy and proceed exactly as for the current secret — the policy
   attached to a row is single per-id, not per-secret, so a caller signing
   with the outgoing secret during the grace window gets the *same* allow
   list as one signing with the current secret.
5. If neither matches, `InvalidSignatureException`, exactly as ADR 0057
   already defines for a signature mismatch.

The grace window is read through `env.wall_now()` (ADR 0051's calendar-time
seam) — never `env.now()` — because `valid_until` is a calendar deadline
supplied by an operator's `grace_secs`, the same class of externally-meaningful
absolute timestamp TTL's `expires_at` already is. `SimEnv` derives
`wall_now()` from virtual time, so a grace-window test is seed-reproducible
exactly like every other `wall_now()` consumer.

**Expired `previous` entries are dropped lazily**, on the next `Put`/`Rotate`
of that row, and by nobody else — there is deliberately **no janitor**. A
`previous` entry past its `valid_until` is inert (step 4 above already
refuses it on the time check), so leaving it in place costs nothing but a
few stale bytes in a row that will be touched again the next time that key
rotates or is redefined. This mirrors the "accepted, not urgent" posture the
backup janitor's own two-phase reclaim takes toward slower cleanup, except
here there is no *need* for even that: an inert row is not consuming a
segment-store byte the way a deleted backup's objects are.

### 4. Static config becomes bootstrap

`--dynamo-auth PATH` and the `dynamo_auth` config section (ADR 0057) keep
working unchanged as a **bootstrap mechanism**: consulted only when a key id
is absent from the replicated catalog (step 1 above), so **the catalog
always wins on a shared id** — an operator can `PutCredential` a new row
under the same access key id a static config entry uses, and from that
point the static entry for that id is shadowed everywhere.

Static keys **cannot be rotated or revoked through the admin API** — they
have no `CredentialRow`, so `RotateCredential`/`RevokeCredential` against a
static-only id fails (there is nothing to rotate; the operator's only lever
is editing the config file and restarting, exactly as today). `animusd`
logs at startup, once, when `dynamo_auth` is configured, that static keys
are a bootstrap mechanism superseded by any catalog row sharing their id —
so an operator who forgets to `PutCredential` a "real" replacement does not
silently keep relying on a file they believe they've retired.

A **`--dynamo-auth-seed`** mode — proposing a static config file's keys into
the catalog once, on a fresh cluster, so an operator can migrate off the
static file entirely with one flag rather than one `PutCredential` call per
key — is a plausible follow-up. It is **not part of this ADR**: recorded
here as a named possibility, not a decision.

### 5. Allow-list enforcement at dispatch

After `sigv4::verify` succeeds (Decision 3), the gate resolves:

- The request's **operation class** (Decision 1's table above).
- The request's **table set** — every table the request actually names:
  `RequestItems` keys for `BatchGetItem`/`BatchWriteItem`, each action's own
  `TableName` for `TransactGetItems`/`TransactWriteItems`, the stream's
  owning table for a Streams-class call, and the backup's recorded source
  table (`BackupRow::table`, ADR 0059 §3) for a `Backup`-class call keyed by
  backup id rather than table name.

The request is denied with `AccessDeniedException` unless the resolved
`Policy` allows the class **and** every named table is matched by
`Policy.tables` (`All`, an exact `Names` membership, or a `Prefixes` match
against the table name). A table-less operation (`ListTables`,
`DescribeLimits`, `DescribeEndpoints`) needs only an enabled key — no class
or table check.

**Enforcement happens exactly once**, on the node that received the
client's signed request. A forwarded write (cross-process leader forward,
root `CLAUDE.md`'s "Runnable node" section) or a relayed control-plane
command carries no client signature at all — it is not re-checked against
the allow list on the receiving hop. This is deliberate, not an oversight:
those hops run over the mutual-TLS intra wire (ADR 0064), which already
authenticates *cluster membership* at the transport; re-deriving a client's
identity from an unsigned internal message would require plumbing the
original caller's key id through every internal protocol message, a much
larger change this ADR does not make. The trust boundary is: SigV4 gates a
client's own request at the edge; TLS gates which processes may speak to
each other at all.

**AWS-faithful error shape** (matching the DynamoDB semantics this ADR
mirrors): HTTP 400, `__type` `com.amazonaws.dynamodb.v20120810#Access-
DeniedException` — this is a service-layer authorization failure, not an
auth-layer failure, so unlike ADR 0057's four SigV4 errors (which use the
`com.amazon.coral.service#…` namespace) `AccessDeniedException` uses the
ordinary DynamoDB-namespace prefix `WireError::to_json` already renders.
Message: `User: <arn> is not authorized to perform: dynamodb:<Operation> on
resource: <table arn>` — `<arn>` is a synthesized placeholder ARN built from
the access key id (`arn:aws:iam::000000000000:user/<access-key-id>`, since
this codebase has no account-id/IAM-user concept beyond the key id itself)
and `<table arn>` a synthesized `arn:aws:dynamodb:<region>:000000000000:
table/<name>` using whatever region string the caller's own credential scope
supplied (ADR 0057: the region is taken from the client verbatim, never
pinned) — good enough to match the *shape* real SDKs and tooling expect
without inventing an account/IAM-identity model this codebase does not have.

### 6. Admin surface

New admin routes (`animusd::admin`, ADR 0020's "pure observer + gated
action" pattern), each proposing the corresponding `MetaCommand` through the
control plane exactly like every other DDL/catalog mutation (relayed to the
leader when the receiving node is not one):

- `GET /admin/credentials` — lists ids, policies, `enabled`, and rotation
  state (`previous.is_some()` plus its `valid_until`) — **never secrets**.
- `POST /admin/credentials` — `PutCredential`.
- `POST /admin/credentials/rotate` — `RotateCredential`.
- `POST /admin/credentials/revoke` — `RevokeCredential`.

`animus-cli credentials list|put|rotate|revoke` wraps these. The
console/dashboard (ADR 0021/0035) gets, at most, a **read-only** listing —
no secret entry form in the dashboard UI; secret material only ever travels
through the admin API directly, by design (fewer surfaces that can leak a
secret into a browser history or a screenshot).

Secrets travel to the admin port in request bodies (`Put`/`Rotate`'s
`secret`/`new_secret` fields). This is accepted under **ADR 0020's
trusted-network posture** (the admin port is operator-network-only, not
Internet-exposed) plus **ADR 0064's TLS** (server-only TLS is available on
the admin port, config-gated) — stated plainly rather than left implicit,
since it is a real exposure and not a hypothetical one.

`OpClass::Admin` exists in the enum (Decision 1) for forward-compatibility
with a future admin-port authentication scheme (roadmap §6 names "admin-port
authentication" as a separate item, Decision 8) but **no route checks it
today** — the admin port has no SigV4 gate at all (ADR 0057's stance,
unchanged by this ADR), so there is nothing yet to enforce `Admin` against.

### 7. Exposure, stated plainly

Secrets in this design live:

- **In memory** on every node that has applied the `Metadata` command
  carrying them (every node, eventually — control and data alike, per ADR
  0038).
- **On every node's system-keyspace disk mirror** (ADR 0038's durable
  per-node `StorageEngine` mirror of `Metadata`).
- **In the control plane's Raft WAL and snapshots** (ADR 0009) — a
  `PutCredential`/`RotateCredential` command's payload is exactly as durable,
  and exactly as replicated, as any other `MetaCommand`.
- **On the intra wire** between control-plane replicas propagating the
  command, and toward every data-only node's `Metadata` mirror poll/long-poll
  (ADR 0030) — mutual TLS when `--tls-*`/`tls` config is enabled (ADR 0064),
  plaintext otherwise, identical to every other `Metadata` mutation's
  exposure today.

**No encryption at rest** for this catalog until S-03 (`docs/roadmap.md`'s
next security item, decided to sequence after S-02 specifically to avoid
three crypto ADRs in review at once). **No hashing is possible**: SigV4
verification is an HMAC computed *from the raw secret* — there is no
password-hashing analogue that would let the verifier confirm a signature
without holding the secret in reversible form, unlike an authentication
scheme that only ever compares a hash. This is the **same exposure class the
static config file already has today, now replicated** — the credential
catalog does not introduce a new *kind* of exposure, it multiplies an
existing one across every node instead of confining it to whichever nodes
happen to carry the config file. That multiplication is exactly what buys
rotation and dynamic-node visibility (Decision 1's whole point), and it is
the trade this ADR makes deliberately, not a gap it failed to notice.

### 8. Out of scope

Restated from the roadmap's boundary (Context) and made concrete:

- **IAM policy language, condition keys, temporary/STS credentials.** A
  `Policy` here is one flat `(tables, ops)` pair — no composition, no
  `Deny` statements, no session tokens beyond what ADR 0057 already
  ignores.
- **Per-item or attribute-level authorization.** A policy scopes a whole
  table, never a key range or an attribute within an item.
- **Audit logging beyond metrics** (Decision 9) — no durable per-request
  authorization log.
- **Tenant namespaces.** A `Policy`'s table scoping is the only
  multi-tenancy primitive this ADR adds; there is no notion of a tenant
  owning a namespace of tables it can create/drop freely within.
- **Admin-port authentication.** `OpClass::Admin` (Decision 1) is reserved
  for it; enforcing it is a separate roadmap item, not this ADR.

## Consequences

- A cluster can now rotate a DynamoDB client credential with **zero
  downtime and no config-file edit**: `RotateCredential` propagates through
  the control plane to every node, old and new secrets are both valid for
  `grace_secs`, and clients cut over on their own schedule within the
  window — the same two-active-keys shape AWS IAM itself uses.
- A cluster can hand out **scoped keys** — read-only, write-only, or
  restricted to a table subset by name or prefix — for the first time,
  closing the "every key can do everything" gap without building an IAM
  clone.
- **Data-only nodes verify and authorize locally**, with no round trip to a
  control-plane leader, exactly like their existing `Metadata` mirror
  already gives them for the schema catalog — the replicated-catalog shape
  pays for itself on every node, not just the leader.
- **The secret-exposure surface widens** from "whichever nodes have the
  config file" to "every node in the cluster, plus the control-plane Raft
  log/snapshots" — stated plainly in Decision 7, not a silent cost. This is
  the deliberate trade for rotation and cluster-wide visibility, closed
  further by S-03 (encryption at rest), sequenced next specifically because
  of it.
- **Existing deployments are unaffected**: a key created with no explicit
  policy keeps the pre-S-02 "all tables, all classes" behaviour, and the
  static `dynamo_auth`/`--dynamo-auth` bootstrap path keeps working
  unchanged, shadowed only where an operator explicitly `Put`s a catalog
  row under the same id.
- `Metadata` and its `MetaCommand` enum widen again, following the exact
  pattern ADR 0013's schema catalog and ADR 0059's backup catalog already
  established — no new catalog *mechanism*, one more replicated map with
  its own `Put`/mutate/`Revoke` commands.
- **Enforcement is single-hop.** A forwarded or relayed internal request is
  not re-checked against the originating client's policy — accepted per
  Decision 5's reasoning (the intra wire's mutual TLS is a different trust
  boundary, cluster membership rather than client identity), but worth a
  reviewer's attention if this design is ever extended toward a
  finer-grained authorization model that assumes per-hop enforcement.

## Testing

- `sigv4_vectors_test.rs` (ADR 0057's AWS test-vector suite) is **untouched**
  — this ADR changes nothing about signature verification itself.
- A `credentials` fault-injection corpus in `animus-control`, modelled
  directly on the backup-catalog corpus (`ANIMUS_BACKUP_SEEDS`): `Put`/
  `Rotate`/`Revoke` proposed under leader kills and leadership changes,
  each converging to the same terminal catalog state regardless of when a
  crash lands relative to commit; the rotation grace window driven under
  `SimEnv` virtual time (`wall_now()`, seed-reproducible) proving the
  window opens and closes at the right instant; idempotence of a replayed
  `Put`/`Rotate` payload.
- A real-thread `animusd` integration test, `tests/dynamo_auth_policy.rs`:
  a catalog-created key works from every node in the cluster once its
  `PutCredential` commits (control and data-only alike); `Rotate` keeps the
  outgoing secret valid within `grace_secs` and rejects it once `wall_now()`
  passes `valid_until`; `Revoke` makes the next request fail from
  `UnrecognizedClientException`; each `OpClass`/table-match shape (`All`,
  `Names`, `Prefixes`) both allowing and denying the expected operations,
  with `AccessDeniedException`'s message shape asserted directly; a static
  bootstrap key still authenticates, and a catalog row created under the
  same access key id supersedes it.
- The `is_relayable_command` allowlist addition for `PutCredential`/
  `RotateCredential`/`RevokeCredential` gets its own regression through a
  follower-connected node, beside `SetTableTtl`/`TagResource`/`SetTable-
  Throughput`'s existing precedent in `tests/schema_ddl_relay.rs`.

## As-built (2026-09-05, S-02 step 3 — allow-list enforcement at dispatch)

Steps 1-2 (the catalog and its admin CRUD) landed as designed. Step 3
landed with a few concretizations this ADR left to the implementation:

- **The gate is one merged function, `animus_node::sigv4_gate::
  merged_sigv4_gate`**, not two sequential lookups — it takes the caller's
  `Metadata` (the catalog) and the static bootstrap map together and
  implements Decision 3/4's precedence in one pass: a row present in the
  catalog for the caller's access key id (enabled or not) is authoritative
  and the static map is never consulted for that id at all; only a
  genuinely absent row falls through to the static map. Verification
  itself still goes through `animus_dynamo::sigv4::verify` unchanged, tried
  once per candidate secret (current, then previous while its grace window
  is open) via a one-entry credential map per attempt — the ADR's own
  "this ADR does not touch canonicalization, the HMAC chain, or clock-skew
  checking" holds exactly. `animus_dynamo::sigv4::parse_credential` is a
  small new export (structural parse only, no store lookup) the merged
  gate needs to recover the access key id *before* it knows which
  candidate secret(s) to try.
- **A lock-free fast path avoids the gate's `Metadata` clone on an empty
  catalog.** `ClusterEdgeState::has_catalog_credentials: Arc<AtomicBool>`
  (recomputed at `ClientCtx` construction, on `index_drain::
  change_consumer_loop`'s own per-tick metadata refresh, and immediately
  after this node's own `PutCredential`/`RevokeCredential` commits) lets
  `dynamo.rs::handle_conn` skip `effective_metadata()`'s deep clone
  entirely when the flag is `false` **and** no static bootstrap map is
  configured — the common case, and every pre-ADR-0066 deployment. Not
  named in the Decision text; added because Decision 3's "the verifier the
  request hits first" reads `Metadata` on every gated request, and a
  cluster with SigV4 enabled purely for its static bootstrap map (no
  catalog rows at all) must not pay a new per-request cost for a catalog
  it never populated.
- **The authenticated principal is `animusd::authz::Principal`**
  (`Unrestricted` | `Scoped{access_key_id, region, policy}`) — the "whatever
  minimal type" Decision 5 anticipated. `region` rides along from the
  caller's own `Credential` scope (parsed once, at the gate) purely to
  build `AccessDeniedException`'s synthesized table ARN without a second
  parse at dispatch time.
- **`RestoreTableFromBackup`/`RestoreTableToPointInTime`'s checked table is
  the *target* table**, not the source — reusing `animus_dynamo::wire::
  Operation::table()`'s own pre-existing convention (that accessor already
  resolves both to their target name, for the unrelated duplicate-name
  check `create_table`/`restore_table_from_backup` share) rather than
  introducing a second table-resolution rule for the same two operations.
  A scoped key can therefore restore *into* a table it may create, and the
  ADR's own listed table set for a `Backup`-class call
  (`BackupRow::table`) is what `DescribeBackup`/`DeleteBackup` (ARN-keyed,
  no target) resolve against instead.
- **`ListBackups`/`ListStreams` with no `TableName` filter are cross-table
  reads, not table-less ones** — the ADR's Decision 1 table-less list
  (`ListTables`/`DescribeLimits`/`DescribeEndpoints`) does not include
  either, and neither can honestly answer "every backup/stream regardless
  of table" for a policy scoped to specific tables. `authz::
  authorize_unscoped` requires `TableMatch::All` **and** the class in
  `policy.ops` for this shape; a table-scoped key gets `AccessDeniedException`
  rather than a silently-narrowed result set. Not spelled out in Decision
  5's text, which only lists per-operation table resolution for the
  named multi-table operations (`BatchGetItem`/`BatchWriteItem`/
  `TransactGetItems`/`TransactWriteItems`) — this is the same shape,
  applied to a filterless list call.
- **DynamoDB Streams (`ListStreams`/`DescribeStream`/`GetShardIterator`/
  `GetRecords`) are gated too**, all under `OpClass::Streams`, table
  resolved from the stream ARN (the first three) or, for `GetRecords`,
  from the shard iterator's own tablet — a sealed shard's catalog row
  (`StreamShardRow::table`) or, for an open shard, the tablet's own live
  `table` field. Mirrors the base item API's own authorize-before-dispatch
  order exactly.
- **`classify`'s exhaustive match is the compiler-enforced guarantee**
  Decision 5 asked for ("every DynamoDB operation maps to exactly one
  class") — `animusd::authz::classify(op: &Operation) -> (&'static str,
  OpClass)` has no wildcard arm, so a future `Operation` variant is a
  compile error there until deliberately classified; `authz::
  authorize_op`'s own table-resolution match is exhaustive the same way.
  `authz::tests::every_operation_classifies_per_adr_0066_decision_1` pins
  every variant's classification against this table.
- **The static `--dynamo-auth` bootstrap credential's `Principal` carries
  no policy at all (`Unrestricted`)** — Decision 6's "the static bootstrap
  credential is unrestricted" is implemented as a variant with no `Policy`
  field, not a `Policy::allow_all()` value, so a bootstrap-authenticated
  request never touches `Policy::allows` at dispatch (structurally
  unrestricted, not merely configured to allow everything).

## Alternatives rejected

- **Hashed secrets** (store a password-hash-style digest instead of the raw
  secret). Rejected: impossible with SigV4 — verification is an HMAC
  computed from the *actual* secret bytes, so the verifier must hold the
  secret in reversible form regardless of catalog design; a hash would make
  every signature unverifiable.
- **Per-node local key files, unreplicated** (each node keeps its own
  credential file, kept in sync by an external tool). Rejected: no rotation
  story better than today's ("edit every file, restart every node"), and a
  data-only node joining after a rotation would need its own out-of-band
  sync mechanism this codebase does not have — `Metadata` replication
  (ADR 0038) already solves exactly this for every other cluster-wide fact.
- **An external IAM-compatible service** (a separate authorization service
  this codebase calls out to, or a full policy-language engine embedded
  here). Rejected: explicitly out of scope per `docs/roadmap.md` §6 — IAM-
  the-service is a managed-service property this project has declared it
  will not build; the flat per-key `(tables, ops)` policy is sized to close
  the gap S-02 actually names (no rotation, no replication, no per-key
  scoping) without growing into that boundary.
