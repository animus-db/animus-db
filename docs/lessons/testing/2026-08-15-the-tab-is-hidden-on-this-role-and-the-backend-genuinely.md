# "The tab is hidden on this role" and "the backend genuinely can't serve this role" are two different claims — verify both against a live cluster, don't assume the first implies the second.

**"The tab is hidden on this role" and "the backend genuinely can't serve
this role" are two different claims — verify both against a live
cluster, don't assume the first implies the second.** Widening the
Streams tab to control-only consoles (ADR 0021 #10) required finding out
which of the four Streams-read ops (`ListStreams`/`DescribeStream`/
`GetShardIterator`/`GetRecords`) a control-only node's admin proxy
(`POST /admin/data/dynamo`) can actually serve, since nothing had ever
exercised that combination (every existing `dynamo_streams.rs` "every
node" test only ever brings up combined-mode clusters). Reading the code
first suggested `ListStreams`/`DescribeStream` were metadata-only (safe)
and the other two needed a local CP data plane (unsafe) — true, but
"unsafe" turned out to mean two *different* failure shapes, not one: (1)
`GetRecords` on a **sealed** shard calls `ClientCtx::data()`
unconditionally, which **panics** — not a returned error, an
empty/dropped HTTP reply (`curl`: "Empty reply from server"), confirmed
live by hitting a real split cluster's control-node admin port and
finding `thread 'tokio-rt-worker' panicked … ClientCtx::data called on a
control-only node` in its stdout, even though `ClientCtx::data()`'s own
doc comment already says this exact call path must never happen; (2) the
**open**-shard path (`GetShardIterator{LATEST}`/`GetRecords`) doesn't
panic but silently stalls for the full `SCHEMA_COMMIT_TIMEOUT` (~10s)
before failing, because a control-only node's `resolve_cp_route` has no
local replica to derive a real leader hint from, so its blind-forward
fallback picks the same (possibly wrong) replica every retry with nothing
chasing the refusal's own hint — confirmed by timing the request (`time
curl` showed `10.03s`) rather than assuming a quick failure. Getting a
genuine sealed shard to test against needed no small `--stream-seal-
bytes` tuning either: `UpdateTable{StreamEnabled:false}`'s F12-b
disable-triggered final seal produces one unconditionally, and it's also
the only seal path any of the deployment CLI's split-cluster modes
(`--cluster-control`/`control --config`/`data --config`) actually expose
today — none of them thread the `_streams`-suffixed seal-knob overrides
the combined-mode `--cluster N`/`--config --node` paths do (a real,
separate documented CLI gap, `animusd/CLAUDE.md`'s own "split-deployment
CLI path is a named follow-up" note). Both findings changed the fix: the
dashboard doesn't try to distinguish "will this shard's `GetRecords` call
panic or hang," it just never dials either op from a control-only
console at all, degrading the whole live-tail section with one static
note — a narrower, correctly-scoped fix than trying to special-case one
of the two failure shapes and being surprised by the other in production.
The backend panic/timeout themselves were **not** fixed in this PR (a
UI-scoped change; see "separate PRs for incidental bugs") but are now
documented in `animusd/CLAUDE.md` and ADR 0021 #10 for whoever picks that
up. (2026-08-15.)
