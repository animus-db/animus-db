# git rerere can resolve a conflict silently; still verify append-only orderings

`git merge` with `rerere.enabled` can print "Resolved ... using previous
resolution" and stage nothing for you to look at. For an append-only list such
as `Metric` / `Metric::ALL` in `animus-env` (slot = index, order is
load-bearing), a replayed resolution may be stale when main appended variants
in the meantime. After any merge touching such a list, check the variant
order against `origin/main`'s (new variant last), the `ALL` length, and the
`name()` arms, rather than trusting the "resolved" line.
