# Boot a sim cluster at an old profile and roll it up the gate ladder

Found while adding the `Gate::MrscReplicaRemoval` (version 7) cells to
`sim_cluster_mrsc`. The first attempt generalised the corpus' `boot` to "every
node plays `Release(7)` and reports `[1, 7]`, then finalize". The cluster never
started the version era (`era never converged: era not active everywhere`), with
every node healthy and all six members registered.

**Why.** `BinaryProfile::Release(n)` models a binary whose own range is
`[n-1, n]` (the N-1/N skew policy), and `Metadata::apply` refuses a
`ReportNodeVersion` whose range excludes the *current* cluster version (1 while the
era is starting). A profile whose floor is above the current version can never
report into a fresh cluster, so the era never starts. The real binary's floor is
held at 1; only the model's is `n-1`.

**Rule.** To get a sim cluster to cluster version `n`, start every node at the
oldest profile that can report (`Release(2)`, `[1, 2]`), then for each step `k`:
play `Release(k)` with `set_node_version(.., [1, k])` on every node, wait for every
record to land at `[1, k]` on every replica, and finalize `k-1 -> k` through the
real admin path (`Run::boot_at` in `sim_cluster_mrsc.rs`). That is also the faithful
rolling-upgrade path, so the cell exercises each gate opening instead of skipping
the ladder. Keep the default boot at its old version so existing cells stay
byte-identical.
