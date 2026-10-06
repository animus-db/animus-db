# A CI failure annotation leads with a compact summary, not the tail of the full log

The chaos job annotated a failure with the last 3,500 characters of
`violations.txt`. One `[eventual-prefix]` line carries two value lists of
hundreds of elements, so the tail was the middle of that list: the decisive
facts (which node, how many values missing, which writer class) were cut off the
front, and the `[replica-convergence]` result and per-node counters, appended
earlier in the file, were not shown at all. The failing mechanism was inferable
only from a hand-copied annotation.

The harness now writes `summary.txt` beside `violations.txt`: a header with
violation counts per kind, one line per violation *group* (kind, key, serving
node, count, first time, missing count, writer classes — never the lists), the
`[replica-convergence]` verdict (printed even when it passes), and every node's
counters, trimmed to fit the cap with the convergence lines and counters always
kept. The workflow annotates `summary.txt` first and takes the **head**, not the
tail, of everything else. Rule: any artifact meant for a size-capped surface is
authored for that surface, with the must-keep lines reserved first; a capped
tail of an uncapped log is not a summary.
