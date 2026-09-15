# Extracting a "pure decision" from a method that intentionally short-circuits an expensive call must preserve that laziness explicitly, or the refactor silently becomes a hot-path perf regression.

**Extracting a "pure decision" from a method that intentionally short-circuits
an expensive call must preserve that laziness explicitly, or the refactor
silently becomes a hot-path perf regression.** `resolve_cp_route` avoided
`RaftNode::metadata()`'s full deep-clone on the common "local leader" /
"known hint" paths by checking cheap facts first; pulling the branching out
as a pure `decide_cp_route` function required the wrapper to keep gathering
metadata-derived facts lazily (only in the one branch that needs them)
rather than eagerly computing everything before calling the pure function.
When extracting logic mechanically, check what expensive input the original
short-circuited around, not just what it decided. (PR #33.)
