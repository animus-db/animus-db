# CLAUDE.md — animus-cli

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The operator/client CLI (`animus`): a thin client over a node's APIs, speaking
two transports —

- a **plain-TCP client API** (`status`/`put`/`get`): one framed
  `ClientRequest`, one framed `ClientResponse`, against a node's **client**
  address;
- an **HTTP/JSON admin** subcommand group (ADR 0020): a minimal hand-rolled
  HTTP/1.0 client (`http_call`) against a node's **admin** address, printing
  the server's JSON verbatim (the server pretty-prints) and exiting non-zero on
  a non-2xx status.

The whole crate is one file, `src/main.rs`.

## TLS (`--tls-ca PATH`, ADR 0064, S-01 commit 2)

Config-gated, default off — omitted, every dial is plain TCP,
byte-for-byte unchanged. `--tls-ca PATH` may appear anywhere in the
argument list (`extract_tls_ca` pulls it out, in place, before any
subcommand's own positional parsing runs) and applies to **every** dial
this invocation makes — the client-protocol port (`status`/`put`/`get`)
and every admin-port call (`http_call`, reached by the flat one-shot
routes and by the `decommission`/`control-add`/`control-remove`/
`control-grow` orchestration functions, all of which now take an explicit
`tls: Option<&tokio_rustls::TlsConnector>` parameter threaded down from
`main`).

This CLI is **never a cluster member** — it verifies the node it talks to
(`build_tls_connector` builds a **server-only** `rustls::ClientConfig`
trusting the given CA) but never presents a client certificate of its own,
matching ADR 0064 Decision 2: every port this CLI dials (`client`/`admin`)
is server-only TLS, never the mutual `internal`/`intra` ports. `animus_env`
is a normal (non-dev) dependency purely for `MaybeTlsStream` (the dial
wrapper `maybe_tls_connect` returns, so `write_frame`/`read_frame`/
`read`/`write_all` all work unchanged) and `tls::server_name_for` (the
identical `ServerName` derivation `animus-env`'s own internal wire and
`animusd`'s intra dialer use) — this crate never constructs a `ProdEnv`
itself. A missing/invalid `--tls-ca` file, or a handshake failure, is a
plain startup/dial error (`main`'s usual `Err(String)` path) — never a
panic.

## Client subcommands

```
animus status <node-addr>
animus put    <node-addr> <table> <key> <value>
animus get    <node-addr> <table> <key>
```

- **Every key names a table** (ADR 0023) — `put`/`get` take a `<table>`
  argument; the table is the routing argument, not part of the key bytes.
- `<node-addr>` is any node's client address (as printed by `animusd`).
  Server-side routing/forwarding means any node can serve any key.
- It depends on `animusd` only for the shared protocol types and frame helpers
  (`ClientRequest`/`ClientResponse`, `read_frame`/`write_frame`) — it does
  **not** speak the internal `Network`.
- `print_response` also handles `ClientResponse::JoinInfo` (ADR 0032): the
  join-discovery reply `animusd join`/`data --seed` consumes at startup —
  not a CLI subcommand of its own. (`ClientResponse::NodeIdAllocated`, ADR
  0036's join-time reply, is **deleted** as of ADR 0040 PR4 — the join path
  no longer has a dedicated wire request/response pair for minting an id at
  all; it claims one via `MetaCommand::RegisterNode`, relayed over the
  already-exhaustive `ProposeSchema`/`Status` primitives, so no new
  `ClientResponse` variant was needed here.) **`ClientResponse` gaining a
  variant means `print_response`'s `match` stops compiling until it's
  handled here too** — this is a separate exhaustiveness site from
  `animusd`'s own `is_relayable_command`/`ClientRequest` dispatch; both need
  checking whenever either enum grows.

## Bulk seeding (`seed`, ADR 0021 amendment)

```
animus seed <admin-addr> <table> <count> [--start N] [--key-prefix P] \
    [--value-bytes B] [--concurrency C] [--batch N]
```

The maintainer-decided home for synthetic bulk data generation: **seeding
is a client concern, not a server use case** (ADR 0021's 2026-09-09
amendment) — `POST /admin/data/seed` used to generate rows *inside* the
node and write them through internal primitives, re-deriving `PutItem`'s
exact byte shapes by hand and silently dropping any ADR 0065-throttled
row. `seed` instead:

1. `DescribeTable`s the target through the admin proxy
   (`POST /admin/data/dynamo {op:"DescribeTable", ...}`, ADR 0021 — this
   CLI has no SigV4 client and never dials the DynamoDB port directly) to
   learn the key schema (`KeySchema` + `AttributeDefinitions`): the
   partition key's name/type, and an optional sort key's.
2. Generates items **client-side**, byte-identical in shape to the old
   server-side seeder's own convention (so an existing dashboard/test
   reading `seed:000000000042` keeps working): `pk = key_prefix +
   zero-padded-12-digit index`, typed per the declared `AttributeType`
   (`S` → string, `N` → the bare index text, `B` → base64 of the prefixed
   text — this crate's own small standard-base64 codec,
   `base64_encode_standard`, since it has no `animus-dynamo` dependency to
   reuse `wire::base64_encode` from); `sk` (composite tables only) = the
   same index, typed the same way, but with **no** `key_prefix` (mirrors
   the deleted `admin.rs::seed_key_attr`'s exact convention); plus a
   filler `payload: {"S": ...}` attribute padded so the serialized item is
   ~`value_bytes` (default 64).
3. Issues real `BatchWriteItem` calls (`POST /admin/data/dynamo
   {op:"BatchWriteItem", ...}`), chunked at `--batch` (default/cap 25 —
   DynamoDB's own per-call limit), with up to `--concurrency` (default 8)
   chunks in flight at once (`futures::stream::buffer_unordered`).
4. Retries a chunk's own `UnprocessedItems` with a bounded exponential
   backoff (`SEED_MAX_RETRY_ATTEMPTS`/`SEED_RETRY_INITIAL_BACKOFF`/
   `SEED_RETRY_MAX_BACKOFF`) rather than reporting it lost immediately — a
   throttled or transiently-refused (mid-split) row gets several chances
   before this command gives up on it.
5. Prints a progress line per completed chunk (running written/unprocessed
   totals) and a final summary (`written`/`unprocessed`/`elapsed`/`keys/s`).
   Returns a nonzero exit only when **nothing** was written at all — a
   partial success (some rows unprocessed) is reported, not a failure.

Unlike every other subcommand's `<node-addr>`, `seed`'s first argument is
an **admin** address (it talks the admin HTTP proxy, never the plain
client protocol) — the same convention every `animus admin <sub>
<admin-addr>` subcommand uses, even though `seed` is a top-level command,
not nested under `admin`, since it is genuinely multi-step orchestration
(chunking, concurrency, retry) rather than a one-shot `(method, path,
body)` dispatch.

`/admin/data/seed` itself is now a thin proxy over this identical
`BatchWriteItem` operation (`animusd::admin::action_data_seed`) — kept
only because the dashboard's own "Seed data" form depends on its existing
request/response contract; see that crate's own `CLAUDE.md` and ADR
0021's amendment for the server-side half.

## Admin subcommand group

`animus admin <sub> <admin-addr> [args]` — see `ADMIN_USAGE` in `main.rs` for
the canonical list. Read-only views:

```
config|status|raft|raftkv|metrics|health <admin-addr>
peers|txns|backups|restores|control-members|storage-control <admin-addr>
lsm|wal <admin-addr> [tablet]
wal-segment <admin-addr> <seg> [tablet]
key <admin-addr> <key> [tablet]
storage-scan <admin-addr> [--tablet <id>] [--start <key>] [--limit <n>]
system-table <admin-addr> [--kind <kind>] [--limit <n>] [--after <cursor>]
credentials <admin-addr>
```

- `peers`/`txns`/`backups`/`restores`/`control-members`/`storage-control`/
  `storage-scan`/`system-table` (U-08(i)) are flat one-shot GETs, same shape
  as `config`/`status`/`metrics` — no orchestration, just `GET` + print. Their
  `(method, path, body)` construction lives in `admin_request`, a pure
  function `run_admin` calls into (pulled out specifically so this parsing is
  unit-testable without a socket — see the `#[cfg(test)]` module at the
  bottom of `main.rs`). `storage-scan`'s `--tablet`/`--start`/`--limit` and
  `system-table`'s `--kind`/`--limit`/`--after` are looked up by
  `flag_value` (an order-independent `--name value` scan over the whole arg
  list) rather than the positional `[tablet]` idiom the single-param storage
  routes use — there's no one mandatory leading arg to anchor a trailing
  positional on when every param is optional and there's more than one of
  them. `credentials` (ADR 0066 §6, S-02 step 2) joins this flat-GET group
  — `GET /admin/credentials`, listing ids/policies/enabled/rotation state,
  **never secrets** (`animusd::admin::credential_row_redacted` is the one
  place that redaction is decided; this CLI just prints the server's JSON
  verbatim, so it's safe by construction, not by its own care).

**`credentials-put`/`credentials-rotate`/`credentials-revoke`** (ADR 0066
§1/§2/§6) are mutating, one-shot POSTs — `(method, path, body)` construction
also lives in `admin_request`, unit-tested the same way:

```
credentials-put <admin-addr> <id> <secret> [--enabled true|false] \
    [--policy-tables all|name1,name2|prefix:pfx1,pfx2] [--policy-ops read,write,ddl,streams,backup]
credentials-rotate <admin-addr> <id> <new-secret> <grace-secs>
credentials-revoke <admin-addr> <id>
```

- `credentials-put`'s secret travels only in the POST body (never a query
  string/path segment, which could land in a proxy or access log) — see
  `admin_request`'s own `credentials_put_never_echoes_the_secret_in_the_path`
  regression. `--enabled` defaults `true`; omitting both `--policy-tables`
  and `--policy-ops` omits the whole `policy` field, letting the server
  default to `Policy::allow_all()` (every table, every class but `Admin`) —
  giving only one of the two flags fills the other in with its own
  `allow_all()`-shaped default (`build_policy_body`'s own doc has the exact
  rule) rather than requiring both to be spelled out together.
  `--policy-tables` is `all`, a bare comma-separated list of exact table
  names, or `prefix:` followed by a comma-separated list of prefixes;
  `--policy-ops` is a comma-separated list of
  `read`/`write`/`ddl`/`streams`/`backup`/`admin` — these six strings (and
  the `{"kind": "all"|"names"|"prefixes", ...}` shape `--policy-tables`
  builds) are this CLI's own small, human-readable wire contract with
  `animusd::admin`'s `policy_json`/`parse_policy` — **not**
  `animus_control::Policy`'s own `#[derive(Serialize)]` JSON (externally-
  tagged enum JSON is a worse shape to hand-build here), and this crate
  has no `animus-control` dependency at all to construct that type
  directly even if it wanted to. `credentials-rotate` is rejected
  server-side (`404`) against an unknown id — there is nothing to rotate.
  `credentials-revoke` is idempotent — revoking an already-absent id is
  still a `200`.

Mutating actions:

```
split <admin-addr> <tablet> <split-key>        # ADR 0028 single-command split
flush|compact <admin-addr> <tablet>
reconfigure <admin-addr> <tablet> <voter,voter,...>
drain <admin-addr> <node-id>                   # ADR 0029/0032 release replicas
drain-status <admin-addr> <node-id>
remove <admin-addr> <node-id>
decommission <admin-addr> <node-id> [--force-control-remove]  # composite, see below (ADR 0037 PR4)
control-add <leader-admin-addr> <node-id> <new-node-admin-addr>      # ADR 0037 PR3 (operator-supplied id)
control-add <leader-admin-addr> <new-node-control-addr>              # hardening PR3 (self-minted id, ADR 0040 PR4)
control-remove <leader-admin-addr> <node-id> [--force]               # ADR 0037 PR3; --force: hardening PR2
control-grow <leader-admin-addr> <node-id> <admin-addr> [<node-id> <admin-addr>...]
```

- `flush`/`compact`/`reconfigure`/`drain`/`drain-status`/`remove`/
  `stream-grow` (roadmap C-04 E2) all build their `(method, path, body)`
  through `admin_request` too, same as every other route on this page —
  each now has its own `admin_request` unit test covering the happy path
  and the argument-error paths the parser already has (a missing arg, a
  non-numeric tablet, `reconfigure`'s own unvalidated trailing-comma
  voter list). `decommission`/`control-add`/`control-grow`/
  `control-remove` above are excluded — real orchestration, not
  `admin_request` arms, so nothing here covers them end to end.
- Metrics/storage views are **per node** (a follower's leader-only counters
  are legitimately 0) — point them at the relevant node.
- **`decommission` is real orchestration, not a one-shot passthrough**
  (`run_decommission`, ADR 0032 PR3): it drives drain → polls `drain-status`
  until the node holds no replicas → `remove`. It must target the
  **control-plane leader's** admin address (the underlying admin actions are
  local-leader-only and not relayed); on a follower it fails with a
  "not the control-plane leader" routing error — retry against the leader.
  **A combined node that is also a *live* control-plane voter (ADR 0037
  PR4)**: `run_decommission` asks `GET /admin/control/members` up front
  (best-effort — an old binary/route-miss just skips this pre-check and
  falls through to the ordinary flow, whose own final `remove` step still
  carries the authoritative refusal); if `node` itself (ADR 0040 PR1: one
  identity per node — there is no more separate control id to derive) is a
  live voter, it refuses immediately (before ever draining) unless
  `--force-control-remove` is passed, in which case it runs `control-remove`
  + polls to convergence *first*, then falls
  through to the unchanged drain → drain-status → remove flow. The
  authoritative refusal always lives server-side in `admin_remove_member`
  (`animusd`) — this is a friendlier, fail-fast CLI-side mirror of it, not a
  replacement.
- **`control-add`/`control-grow` are also orchestration, not passthroughs**
  (ADR 0037 PR3): the operator-supplied-id form (3 args) takes the new
  control voter's own **admin** address (this CLI's convention everywhere
  else), not the internal control-Raft address `POST
  /admin/control/member/add`'s wire payload actually wants — `run_control_add`
  bridges the two itself via a `GET /admin/config` against the new node
  (which doubles as the "confirm it's up" liveness check), then polls the
  **new node's own** `/admin/control/members` until it reports itself a
  voter. `control-grow` is a sequential loop of `control-add` calls (one
  server at a time — `RaftCore::change_membership` never accepts a
  multi-server delta), each waiting for its own catch-up before the next.
  `control-add`/`control-remove`/`control-grow` all target the **leader's**
  admin address, same "not relayed" discipline as `decommission`.
- **`control-add` also has a 2-arg, self-minted-id form** (the ADR 0037
  hardening trio's PR3, closing ADR 0037's own "Coordination with ADR 0036"
  deferral — locked decision: disambiguate by **arity**, no `--auto` flag;
  re-based onto ADR 0040 Decision B/C's self-minting + registration CAS in
  PR4, replacing the ADR 0036 allocator this originally wired into):
  `control-add <leader-admin-addr> <new-node-control-addr>` — note the single
  positional here is the new voter's **internal control-Raft** address
  directly, *not* an admin address to resolve via `/admin/config` like the
  3-arg form: there is no id yet to look a running node up by (the id is
  self-minted server-side, inside the same admin call, via `NodeId::mint` —
  `run_control_add_allocated`). It prints the minted id and returns — there
  is no known admin port to poll for catch-up convergence, since the
  physical process at that address may not even be running yet: the
  operator's next step is to start it there with `--id <minted-id>` (e.g.
  `animusd join --seed <any-node> --id <minted-id> --base-port <port>`).
  `run_control_add`'s own operator-supplied form is unchanged except that
  the id is now re-validated via `NodeId::propose` and the old
  "`ALLOC_ID_BASE`-range" refusal is gone (no ranges exist anymore).
- **Dynamo-proxy wrappers** (roadmap U-08(ii)): `backup-create`/
  `backup-delete`/`restore`/`pitr-enable`/`pitr-disable`/`ttl`/`stream` are
  the last flat mutating group — each a thin `POST /admin/data/dynamo
  {op, payload}` (ADR 0021), reusing the exact wire shapes the dashboard
  already sends for these same actions (`dashboard_backups.js`'s
  `createBackup`/`deleteBackup`/`restoreBackup`/`togglePitr`,
  `dashboard_browser.js`'s `enableTtl`/`disableTtl`/the stream row's
  enable/disable calls) rather than inventing a second wire contract for
  the same six DynamoDB operations:

  ```
  backup-create <admin-addr> <table> <backup-name>       # CreateBackup
  backup-delete <admin-addr> <backup-arn>                # DeleteBackup
  restore <admin-addr> <backup-arn> <target-table>        # RestoreTableFromBackup
  pitr-enable|pitr-disable <admin-addr> <table>           # UpdateContinuousBackups
  ttl <admin-addr> <table> <attribute> [--disable]        # UpdateTimeToLive
  stream <admin-addr> <table> <VIEW_TYPE|off>             # UpdateTable{StreamSpecification}
  ```

  `ttl` (bare) was deliberately left unclaimed by `ttl-reaper`'s own GET
  route (see that arm's own comment in `admin_request`) for exactly this
  wrapper. `stream`'s `<VIEW_TYPE>` is one of DynamoDB's four real
  `StreamViewType`s (`NEW_IMAGE`/`OLD_IMAGE`/`NEW_AND_OLD_IMAGES`/
  `KEYS_ONLY`) or the literal `off` to disable — validated client-side
  (an unknown view type is a plain CLI error, not a round trip that comes
  back a wire-level `ValidationException`). None of these six ops needed
  a proxy allow-list change — `animusd::admin::action_data_dynamo` has
  none beyond the bare-name Streams-vs-item disambiguation, and none of
  these are Streams ops. `admin_request`'s own unit tests cover the happy
  path, a missing-argument error, and the `--disable`/`off` variants for
  every one of the six.
- **`export-create`/`export-describe`/`export-list` (ADR 0068, S-05 PR 1)**
  join the same dynamo-proxy group, one more `POST /admin/data/dynamo
  {op, payload}` triple over the new `ExportTableToPointInTime`/
  `DescribeExport`/`ListExports` wire operations:

  ```
  export-create <admin-addr> <table-arn> <s3-bucket> [s3-prefix]   # ExportTableToPointInTime
  export-describe <admin-addr> <export-arn>                        # DescribeExport
  export-list <admin-addr> [table-arn]                             # ListExports
  ```

  `export-create` takes a table **ARN**, not a bare table name, matching
  the real `ExportTableToPointInTime` wire shape (`animus_dynamo::wire::
  table_arn` mints one from a bare name if a caller has only that);
  `s3-prefix` is optional, omitted from the payload entirely when absent
  rather than sent as an empty string. No `ExportTime`/`ClientToken`
  flags yet — this wrapper covers the common case (a fresh export to a
  customer bucket, no idempotency token needed for a one-shot CLI
  invocation). Like the six wrappers above, no proxy allow-list change was
  needed (none of these three are Streams ops), and `admin_request`'s own
  unit tests cover the happy path, the optional-prefix omission, and a
  missing-argument error for all three.
- **`import-create`/`import-describe`/`import-list` (ADR 0068 §6, S-05 PR
  2)** are the mirror-image trio, over `ImportTable`/`DescribeImport`/
  `ListImports`:

  ```
  import-create <admin-addr> <table> <s3-bucket> [s3-prefix] [--gzip|--none] \
      --pk name:TYPE [--sk name:TYPE]                                # ImportTable
  import-describe <admin-addr> <import-arn>                          # DescribeImport
  import-list <admin-addr> [table-arn]                               # ListImports
  ```

  **`import-create` takes a bare table *name*, not an ARN** (unlike
  `export-create`) — the target table does not exist yet at call time, so
  there is no ARN to name it by until this call creates it. `--pk
  name:TYPE` is required (this wrapper builds a minimal single-hash-key
  `TableCreationParameters`; no GSI/throughput flags yet — the common
  case, matching `export-create`'s own "covers the common case" scope);
  `--sk name:TYPE` adds a composite sort key. `[s3-prefix]` is positional
  like `export-create`'s, disambiguated from the flags that follow it by
  checking for a leading `--` (an arg starting with `--` at that position
  means the prefix was omitted, not supplied as `"--gzip"` or similar).
  Compression defaults to `GZIP` (this adapter's own export job — and
  real AWS's own export tooling — always gzips); `--none` requests
  `InputCompressionType: "NONE"`. `admin_request`'s own unit tests cover
  the happy path (with and without a sort key/prefix/`--none`), the
  missing-`<table>`/`<s3-bucket>`/`--pk` errors, and a malformed `--pk`
  (no `:TYPE`).
- **`control-remove ... [--force]` (ADR 0037 hardening PR2, PR #136, the quorum-guard
  liveness fix)**: the server now refuses a removal that would leave fewer
  than a majority of the *resulting* voters reachable (per
  `RaftNode::control_peer_believed_alive`, a control-id-native liveness
  signal — see `animus-control/CLAUDE.md`'s `node.rs` entry), naming the
  apparently-dead voter(s) and pointing at `--force`. `--force` bypasses that
  guard — the same explicit escape hatch `remove`/`decommission` already use
  for their own hard cases. **`decommission --force-control-remove` does
  NOT imply `--force`** — these are deliberately separate, independently-
  explicit flags: the former only says "run `control-remove` as part of
  decommission," never "and skip `control-remove`'s own safety check."
  `run_decommission`'s internal `control-remove` call always passes
  `force: false`; if the liveness guard refuses it, the operator must retry
  with `animus admin control-remove <leader> <id> --force` by hand.

## Tests

The client path (`status`/`put`/`get`) and the admin surface's actual HTTP
behavior have no tests of their own here; they're covered end-to-end by
`animusd`'s `tests/cluster.rs`, and by `animusd`'s admin/decommission tests
(`tests/decommission.rs` among others). `main.rs` does carry a `#[cfg(test)]`
module (`cargo test -p animus-cli`) for `admin_request` — the pure
`(subcommand, args) -> (method, path, body)` step of `run_admin`'s flat GET
**and POST** dispatch (every mutating one-shot route, including the U-08(ii)
dynamo-proxy wrappers above, builds its request through this same function)
— and its `flag_value` helper; it opens no sockets. No `animusd`
integration test exercises the real `animus` binary end to end — `animusd`
has no dependency on `animus-cli` at all (`tests/control_membership_admin.rs`
mirrors `internal_addr_from_admin_config`'s key path rather than calling
it) — so these parser-level unit tests are this crate's only coverage of
the six new wrappers; the underlying `/admin/data/dynamo` route itself is
exercised by `animusd`'s existing dashboard-action tests
(`dashboard_endpoint.rs`'s U-02/U-04 cases) and the DynamoDB wire suites
those ops already have (`dynamo_backup.rs`, `dynamo_restore.rs`,
`dynamo_pitr_restore.rs`, `dynamo_ttl.rs`, `dynamo_streams.rs`), unrelated
to this crate. Also
`extract_tls_ca` (found anywhere in args / absent / trailing with no
value) and `build_tls_connector` (rejects a missing file and a file with
no certificates) — pure/local-filesystem-only, no socket, no live TLS
handshake; `animusd`'s `tests/tls_e2e.rs` is the real end-to-end
regression net for a genuine `--tls-ca` dial against a live node.

**`seed`'s own request-shaping/item-generation logic** (ADR 0021
amendment) is unit-tested the same pure, socket-free way: `seed_item` for
an `S`/`N`/`B`-typed hash key and a composite key (the sort key gets no
`key_prefix`), `parse_key_schema` (`DescribeTable`'s `KeySchema`/
`AttributeDefinitions` decoded into `(pk name, pk type, sort)`, including
the undeclared-attribute-type default and the missing-`KeySchema` error),
`seed_chunk_bounds` (splitting at the batch cap, including the
under-one-batch and zero-row edges), `describe_table_request_body`/
`batch_write_request_body` (the exact `/admin/data/dynamo` body shape),
`unprocessed_items` (recovering a chunk's own retry set from a
`BatchWriteItem` response's `UnprocessedItems`), and
`base64_encode_standard` (RFC 4648 §10's own test vectors). No integration
test exercises `run_seed`/`submit_seed_chunk` end to end against a real
cluster — this crate has no `tests/` integration-test tree at all (see
this file's own note above), and `animusd` has no dependency on
`animus-cli` to drive one from that side either; the underlying
`BatchWriteItem`/`DescribeTable` wire operations `seed` itself calls
through the admin proxy have their own end-to-end coverage in `animusd`'s
DynamoDB wire suites, and `/admin/data/seed`'s own server-side proxy
behavior (the same operation, driven from the dashboard instead of this
CLI) is covered by `animusd`'s `tests/admin_endpoint.rs`/
`sim_cluster_admin_actions.rs`.
