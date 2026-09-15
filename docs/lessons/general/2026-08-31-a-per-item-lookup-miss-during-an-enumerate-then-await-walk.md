# A per-item lookup miss during an enumerate-then-await walk can mean "already accomplished by a concurrent actor," not "was never real" — check whether an earlier-caught version of the same race already has a skip to reuse (issue #454, `ClientCtx::grow_stream`)

`grow_stream` (`animusd/src/schema.rs`) enumerates a table's tablets from a
`Metadata` snapshot up front, then calls `grow_stream_tablet` on each —
one real `await` per iteration (resolve leader → propose → poll), so wall
time and Raft activity pass between the snapshot and any one tablet's own
turn. A tablet snapshotted `Active` can be retired by a **cascade** split
(a different tablet's cutover in the same walk, or a fully independent
one) before its turn arrives; the lookup `grow_stream_tablet`'s own
`trigger_split` call performs then returns the generic "no such tablet"
message, and — before this fix — that flowed straight through as a hard
`error`, even though the code already had an identical-shaped skip
(`STREAM_GROW_MID_SPLIT`) for a tablet caught `Splitting`/`Building` a
*beat earlier* in the very same up-front `match`. The fix is not a new
concept, just recognizing that the vanished-tablet case is the SAME
concurrent-completion race observed one step later: the tablet didn't stop
existing, it finished being split, and its (now `Active`) children's
lineage already covers stream continuity exactly as it does for any other
split — so skipping this call's own attempt on it costs nothing (the
children just wait for the *next* `grow_stream` call, same as a
`Splitting` parent's own children do today).

**General lesson**: whenever a loop enumerates a set from a stale
snapshot and then does real (`await`-crossing) work per item, a per-item
"this no longer exists" result during the walk is not automatically an
error — it can be the exact same "someone else already finished this"
signal an *earlier*-observed variant of the race already has a legitimate
skip for. Before writing a new error/skip classification, grep the
sibling code path that already classifies the same job's more-obviously-
racy states (here, the up-front `Splitting`/`Building` match) and ask
whether the missing case is really a later-observed instance of the same
thing — reuse its outcome and its safety argument rather than inventing a
parallel one. Keep the interception narrow: match the *exact* error
message/variant produced by the specific call this walk makes, at the one
call site that walk owns, not a blanket catch on that message everywhere
it can be produced — `trigger_split`'s own "no such tablet" is a correct,
unrelated hard error for its other two callers (`auto_split_loop`,
`POST /admin/tablet/split`), which never enumerate a stale snapshot and so
have no such race to excuse. A pure `ClientResponse -> ClientResponse`
classifier extracted into its own function made this reclassification
unit-testable without any `SimEnv`/`ProdEnv` harness at all — the existing
regression coverage for this exact race
(`cascade_split_walks_the_grandparent_chain_with_closed_shard_shape`) is a
real-thread `ProdEnv` e2e test whose relevant window is a small, non-
deterministic timing race, unsuitable as the *only* proof the fix holds.
