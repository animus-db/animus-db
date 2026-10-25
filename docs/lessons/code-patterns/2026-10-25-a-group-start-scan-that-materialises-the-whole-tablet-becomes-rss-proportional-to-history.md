# A group-start scan that materialises the whole tablet becomes RSS proportional to history

Two 8-hour real-process soaks failed only on the RSS trend: per-node RSS climbed 87 -> 150 MB although every node
was kill-restarted every 1-2 epochs and on-disk data was only 4 -> 10 MB. The live heap was never the problem
(`malloc_info` on a running node: ~7 MB in use, 20-30 MB freed-but-held; after a 45k-transaction load, 10 MB in use and
290 MB held). The cause was two **transient** whole-range materialisations at every group start:
`StorageScope::has_data` (the reconciler's `gather_facts`) and `rebuild_txn_tracker`, each a single
`storage.scan(start, end)` that returns every row of the tablet at once (the LSM builds a merged `BTreeMap`, then a
`Vec`): about 1.3 KB per transaction of history, 57 MB twice at 45k transactions. It is freed straight away, but
glibc keeps the high-water mark as RSS, and the history (txn records, resolved markers, base rows) is never garbage
collected, so every restart re-raises the mark a little higher.

How it was found (reusable): (1) compare RSS **at boot** with the history size across restarts, not RSS over time;
(2) `gdb -p PID -batch -ex 'call (int)malloc_info(0, stdout)' -ex 'call (int)fflush(0)'` separates in-use heap from
freed-but-retained (the stdout is the node's log file); (3) a single node under `valgrind --tool=massif --depth=200`
(the default depth of 40 stops inside tokio's frames and attributes everything to the scheduler) named the callers.
Do not poll an endpoint that clones the structure you are measuring: `/admin/txns` serialises every tracked
transaction and added 10-60 MB of its own transient.

Rule: a scan that runs once per group start must be bounded in what it holds at once, not just in how often it runs.
Visit the range in key-byte sub-ranges (`scan_chunk_ranges`, tokens are uniform so ~1/256 of the data each) and drop
each chunk before the next; answer presence checks (`has_data`) from the first chunk that decides. A fix that keeps
the result (the tracker itself, ~175 B per decided transaction) is a separate problem: see issue #1380.
