# A catch-up convergence predicate needs a liveness bound, and the load that breaks it is *sustained*, not bursty — bench with a continuous writer

**A catch-up convergence predicate needs a liveness bound, and the load
that breaks it is *sustained*, not bursty — bench with a continuous
writer** (ADR 0050 Train B rung 8, 2026-08-17). The split driver's
"converged = the latest tail pass shipped zero new records" was green
through every e2e (their racing writes were finite bursts) and
structurally un-satisfiable under a continuous sequential writer: every
200ms tick's pass found that tick's own fresh writes, so the hot tablet
— exactly the one that needs splitting — could never freeze. The rung-8
bench (a *continuous* writer, not a burst) found it on its first run;
the fix is a bounded chase (`SPLIT_MAX_TAIL_PASSES`) because the
workflow's correctness never depended on the lag being zero — only the
cutover blip's size did. Same run, same shape one layer down: the
unfiltered final image re-shipped the whole table *inside the freeze
window* (blip scaling with table size, not write rate) — fixed by a
pre-bulk version floor. General form: for any "quiesce then flip"
workflow, ask (1) can the quiesce condition ever be satisfied under
worst-case sustained load, and (2) what inside the flip window scales
with total size rather than recent activity; a bench with a
continuous-load client answers both where burst-shaped e2es answer
neither.
