# A per-entity "do files exist" probe on a flat directory must not list the directory

Issue #1303. Per-tablet engines share one flat data dir and are identified by
filename prefix (ADR 0050, and the layout is durable under ADR 0073). The
factory's `probe`/`destroy`/`local_tablets`, and `LsmEngine::open`'s orphan-WAL
cleanup, each did a whole-directory `Disk::list`, so hosting N tablets was
O(N^2): CPU per replica went 13 ms at G=300 to 40 ms at G=1000, and G=2000
never finished.

- Cost grows with the *directory*, not the entity: any per-entity call that
  lists a shared namespace is a hidden quadratic. Grep for `.list()` on
  per-entity paths when adding a per-entity seam.
- When the layout cannot change, cache one listing and keep it exact by
  recording the only ways files can appear afterwards (an engine open/clone
  through the same factory), re-listing only for the unresolved ones.
- A failed listing must not be cached as "empty": it reads as "nothing found"
  for that call only.
- Prove scaling deterministically (count `Disk::list` calls and entries through
  a counting `Disk` over `SimEnv` at N and 10N, with the old shape as a negative
  control), not by wall time.
- Code-reading guesses about the cost were partly wrong (tokio caches
  `file_type()`); measure first.
