# An introspection surface whose cost scales with the data it describes needs an explicit answer to "what happens when something polls this every few seconds?" — because a dashboard eventually will

**An introspection surface whose cost scales with the data it describes
needs an explicit answer to "what happens when something polls this every
few seconds?" — because a dashboard eventually will** (2026-08-19).
`/admin/raftkv` computed `key_count`/`byte_size` by materializing every
hosted tablet's rows per request. Its own doc comment blessed this —
"this is a debug surface, so the materialize-then-count cost is
acceptable" — and that was *true when written*, for a human occasionally
curling a route. It stopped being true when the Console started fetching
the same route from every node on a 5s auto-refresh, and nothing
re-examined the original judgement, because the cost lives in one file
and the polling lives in another. Measured: with a 20,000-row table
mid-split, polling it every 3s stretched the split's build from 4.5s to
41.8s (~9x). **The pathology is specifically an observer that perturbs
what it observes** — the operator watching a slow split was making it
slower, and every "why is this slow?" measurement taken through that
surface was measuring partly itself. I hit this as a *debugging* failure
before finding it as a bug: my own sampler polled the same route, so my
first published wall-clock numbers for an unrelated fix were ~2x
inflated and had to be corrected afterward. **Rules.** (1) When you add
or bless an O(dataset) read on a debug route, write down what polls it;
if the answer is "a UI, on a timer," it needs a cheap default and an
opt-in exact path (`?exact=1`), not a comment saying the cost is
acceptable. (2) When measuring anything, account for your own
instrument: prefer counters the system already maintains
(`storage_sstable_block_reads` made this both diagnosable and testable)
over polling a rich endpoint in a loop, and when you must poll, measure
the same workload unpolled to size your own footprint. (3) A cheap
estimate that the *enforcement* path already uses (here `auto_split_loop`'s
own `approx_key_count`/`approx_bytes`) is often the better default even
ignoring cost, because it makes the UI agree with the mechanism instead
of showing a truer number the system never acts on.
(`crates/animusd/src/lib.rs::CpGroup::raft_view`, `admin.rs::raftkv_view`.)
