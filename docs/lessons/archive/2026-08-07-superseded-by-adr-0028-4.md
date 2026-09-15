# Superseded by ADR 0028

**Superseded by ADR 0028**: `current_split_bound`/`SPLIT_BOUND_KEY` and the
"a group can be split more than once" data-plane mechanism this entry
discusses are deleted — split no longer has a data-plane half at all.
Retained for historical record. **Before reaching for "remember everything" to disambiguate an edge case,
check whether a cheap, independent check at the point of irreversible action
can bound the state to O(1) instead.** Lifting the CP-data "a group can only
ever apply one `Split`" limit (letting a tablet reshard repeatedly as it
regrows) initially seemed to need a full per-split history: a caller
confirming "did my key `K` ever apply" needs a real answer even after a
*later* split narrows past `K`, and a single "current boundary" value can't
tell "K applied, then something narrowed further" apart from "K never
applied, something else did instead." A full history answers that
unambiguously — but it's unbounded (grows with every split a lineage ever
does, for the life of the process) and, worse, quadratic to maintain (each
split re-persists the *whole* history, so split N pays to rewrite N-1 prior
entries) — the same shape of hazard as the control plane's old
"re-serialize-per-chunk" election-storm bug. The actual fix: keep only the
current boundary (O(1), forever), accept that the confirm signal is
sometimes ambiguous, and add a **second, independent, cheap check at the one
place that does something irreversible** (deleting a tablet) — verify the
tablet isn't still locally, genuinely hosted before ever touching it. A
wrong call into the *heuristic* signal now just skips a cleanup opportunity
(the original, already-tolerated "orphan lingers" outcome); only a wrong
call into the *hard gate* would cause real harm, and that check is simple
enough to get right by inspection. Generalizes: when the cost of
"remembering enough to always be certain" is unbounded growth, ask whether
the actual danger is concentrated at one action (here, deletion) — if so,
guard *that action* directly instead of trying to make the upstream signal
perfectly precise. (`animus-cp-data` `current_split_bound`/`SPLIT_BOUND_KEY`;
`animusd` `drop_orphan_tablet`'s local-hosting gate.)
