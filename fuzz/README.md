# fuzz/ — fuzzing every untrusted parser (roadmap R-01 (c))

A [cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz.html) project, **outside the
root workspace** (own `[workspace]`, root `exclude = ["fuzz"]`): `cargo build --workspace`,
clippy and `cargo deny` never see it. The property for every target is the same:
**never panic, never hang, never allocate unboundedly; decode returns `Ok` or a named error.**

## Targets

Multi-decoder targets take `<decoder-name>\n<body>` (an unknown name is hashed onto a
decoder, so no input is wasted); seeds are therefore readable text/hex-able files.

| Target | Covers |
|---|---|
| `dynamo_request` | `animus_dynamo::wire::decode_request` for **every** supported operation (and so the private `UpdateExpression` / `ConditionExpression` / `ProjectionExpression` / `KeyConditionExpression` parsers, reached through the JSON body), `streams_wire::decode_request`, `decode_item`/`encode_item`, `decode_stored_item`, shard-iterator / sequence-number / shard-id / stream-ARN / table-ARN parsers. A decoded request is then *used*: `apply_update`, `ConditionExpression::evaluate`, `project`, `SortKeyCondition::matches`. |
| `dynamo_expressions` | Structure-aware: five `\n`-separated expression strings embedded (JSON-escaped) into `UpdateItem`/`GetItem`/`PutItem`/`DeleteItem`/`Query`/`Scan`/`TransactWriteItems` bodies with fixed `ExpressionAttributeNames`/`Values`, so the fuzzer spends its budget in the expression grammars instead of JSON syntax. |
| `partiql` | The PartiQL lexer/parser (`parse_statement`, ADR 0071), every `lower_*` (incl. the `TransactGet`/`TransactAction` forms) over the parsed statement, and the `NextToken` decoder. |
| `http_sigv4` | The HTTP request-head parser (`animus_node::http::parse_request_head`, `query_param`, `percent_decode`) and, on whatever parses, SigV4 `Authorization` parsing (`parse_credential`) and full `verify` (canonical request, string-to-sign, skew) at three clock values. |
| `item_codecs` | `animus-item`: stored-item codec, `ChangeRecord` (versioned decode + re-encode), footprint, `numkey` encode/decode, GSI/LSI row-key parsers. |
| `net_frames` | `ClientRequest`/`ClientResponse` JSON frames, the connection-handshake preamble + extension area (`NHS1`/`CHS1`), `check_peer`, the not-leader refusal parser, the frame-length gate. |
| `lsm_formats` | LSM WAL (whole file v1/v2 dispatch, bare record, and a re-framed variant that gets past length/CRC), manifest, SSTable data block, block index (incl. CRC re-stamped), and a whole SSTable image opened/scanned/point-read through a `SimEnv` disk. |
| `control_formats` | Control Raft WAL (`CWL1`, v1/v2, with CRCs re-stamped so the payload decoder runs), shared WAL (`SWL1`), control snapshot image, `Metadata::from_json`, system-keyspace key decoders. |
| `cp_data_formats` | RaftKV wire frame, snapshot image, WAL (`PersistedState<KvCommand, KvState>`), segment codec (+ `decode_and_slice`), backup data chunk and manifest, engine key-layout marker, cursor decoders, engine-internal marker values (txn record, split, ceiling, seal, applied). |
| `encryption_envelope` | The `ADE1` envelope: whole-file scan (header/version dispatch, per-frame length + authentication, torn tail vs corruption) under the fixture key, the `EncryptedDisk` `size`/`read`/`read_at` paths over a file holding the fuzzed bytes, and the whole-object opener used by the encrypted `SegmentStore`. |

Private decoders are reached through off-by-default, `#[doc(hidden)]` `fuzzing` Cargo
features (`animus-storage`, `animus-cp-data`, `animus-env`): thin entry points, no
behaviour change, never enabled by a production build. Prefer an existing `pub` API when
adding a target.

Deliberately **not** covered: `animusd`'s `ClusterConfig` and the operator CRD (trusted
operator input, and `animusd` is a heavy dependency); the S3 XML responses
(`animus-s3`, parsed from the configured backend, not a client); `mirror::apply_key_write`
(its `.expect`s on a corrupt value are by design for node-local mirror data — see
`known-issues.tsv`'s header and the R-01 report).

## Running

Stable, no libFuzzer — the deterministic smoke (what the per-push `smoke-stable` job runs):

```sh
cd fuzz && cargo test --release --test smoke -- --nocapture
ANIMUS_FUZZ_SMOKE_ITERS=300000 cargo test --release --test smoke   # deeper
ANIMUS_FUZZ_REPLAY=<target>:<hex> cargo test --release --test smoke  # replay one input
```

Every seed plus `ANIMUS_FUZZ_SMOKE_ITERS` (default 3000) mutations per target from a fixed
seed (`ANIMUS_FUZZ_SMOKE_SEED`; `SplitMix64`, never `thread_rng`). A panic prints the target,
the input as hex, the panic message and its location, and fails the test at the end.

Real libFuzzer (nightly + `cargo install cargo-fuzz --locked`; the repo pins 1.96, so use `+nightly`):

```sh
fuzz/seed-corpus.sh                       # corpus from the golden fixtures + fuzz/seeds/
cargo +nightly fuzz list
cargo +nightly fuzz run lsm_formats -- -max_total_time=60 -timeout=20 -rss_limit_mb=2048 -max_len=65536
cargo +nightly fuzz run dynamo_expressions -- -dict=fuzz/dict/dynamo.dict
cargo +nightly fuzz tmin <target> fuzz/artifacts/<target>/crash-<hash>   # minimize a crash
```

Use a separate `CARGO_TARGET_DIR` for the instrumented build if you also build normally.

## Seeds

* `seeds.tsv` — `target<TAB>decoder-name<TAB>path`: the ADR 0073 golden fixtures under
  `crates/*/tests/fixtures/formats/`, read **in place** (nothing is copied into the repo;
  `seed-corpus.sh` materialises `fuzz/corpus/`, which is git-ignored).
* `src/seeds.rs` `derived()` — slices of those fixtures aimed at inner decoders (an SSTable's
  block-index region and first block, each WAL record payload, each RaftKV wire frame).
* `seeds/<target>/*` — small hand-written text seeds for parsers with no fixture.

A new durable format (ADR 0073 checklist) should add its fixture directory to `seeds.tsv`
and, if its decoder is private, a `fuzzing` shim + a `route` name in the matching target.

## CI

`.github/workflows/fuzz.yml`: `smoke-stable` and `libfuzzer-smoke` (60 s per target) on every
push/PR touching `crates/**` or `fuzz/**`; `libfuzzer-nightly` (default 20 min per target, one
job each, corpus carried between nights through the Actions cache) on a schedule. A crash uploads
`fuzz/artifacts/` and goes red.

## When a target finds a crash

Green is an invariant. Minimise the input, file an issue (target, hex input, panic message and
location), fix it in its own PR with a regression test next to the code, and only then let the
input join the corpus. If the fix cannot land in the same PR that found the crash, add a row to
`known-issues.tsv` (target, panic-message substring, issue link) so the smoke reports but does not
fail on it, and delete the row in the fix PR. Never edit or delete a golden fixture to make a
fuzz finding go away (ADR 0073).
