# A cross-region LWW stamp is computed at apply from the stored stamp, and every base-row writer must go through it

When a row carries a last-writer-wins stamp (MREC `MrecVersion`), compute a local
write's stamp **at apply** as a pure fold over `(entry, stored stamp)`
(`max(wall, stored.wall)`, bump `logical` unless the clock is strictly ahead, local
region id) instead of at propose. Every replica then writes identical bytes, a local
write made after observing a remote one beats it even under a slow or skewed clock, and
nothing depends on a clock state. The same idea makes a replicated write a pure
function: applied iff `ver > stored`, else no writes at all (not even a change record,
or a re-delivery would re-fire streams).

The trap is the writers that never reach apply: an edge-valued fast arm (the value is
computed at the edge with no read) or a raw client write would store an unstamped row,
which compares as zero and silently loses every conflict. Close them structurally (make
the fast-arm predicate true for the table kind, refuse raw writes at the choke point)
and pin the guard with a test, not a comment. Also give a convergence proptest a
negative control per rule (arrival-order LWW, no tiebreak): an oracle that cannot be made
to fail is not evidence.
