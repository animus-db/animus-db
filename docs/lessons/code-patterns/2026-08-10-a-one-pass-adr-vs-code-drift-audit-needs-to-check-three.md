# A one-pass ADR-vs-code drift audit needs to check three distinct places for each "future work"/"deferred" claim, not just the ADR line the grep hit — and a claim can go stale for two structurally different reasons.

**A one-pass ADR-vs-code drift audit needs to check three distinct places for
each "future work"/"deferred" claim, not just the ADR line the grep hit —
and a claim can go stale for two structurally different reasons.** Sweeping
every ADR in `docs/adr/` (2026-08-10), the two reasons split cleanly: (1)
**shipped-since** — the gap was closed by later work and the ADR was simply
never revisited (ADR 0006 kept "keyspace metadata replication is future
work" and "`ALTER TABLE` is a non-atomic drop+recreate" stale for two full
ADRs' worth of shipped work, even though the *sibling* ADR that closed both
gaps — 0013 — was itself already correctly updated; same pattern hit two
crate `CLAUDE.md`s, `animus-cql` and `animusd`, which described the
pre-ADR-0013 per-process keyspace tracking well after the code moved to a
replicated `MetaCommand::CreateKeyspace`/`ReplaceTableSchema`). (2)
**superseded-by-deletion** — the ADR describes a mechanism whose *entire
subsystem* was later deleted (ADR 0005's residency-enforcement-via-hinted-
handoff and ADR 0010's read-repair/anti-entropy/`HintStore` both live
entirely in the leaderless AP data plane, physically removed per ADR 0019;
their "still deferred" bullets describe gaps in code that no longer exists,
which is a different fix — a pointer note, not a "shipped" rewrite — from
class (1)). A **third**, easy-to-miss failure mode is *intra-document*
staleness: ADR 0017 had a leftover "Implemented now (Stages A+B)... **Not
yet:** dynamic membership, tablet split" recap sentence that was never
updated when the *same file*'s later sections marked Stages C and D ✅ Done
— so an audit must diff every "not yet" claim against the rest of *its own
document*, not just against other ADRs/CLAUDE.md files. Method that worked:
grep every ADR for forward-looking language, then for each hit check (a) the
same file's own later sections/amendment blocks (several — 0016, 0021, 0024,
0026, 0030, 0035 — already self-correct via inline "✅ Done"/"Amended by ADR
NNNN" notes, so a naive line-number grep would falsely flag them), (b) the
actual code via grep for the mechanism's likely name, and (c) whether the
described subsystem still exists in `crates/` at all. Conservative default:
when a claim is genuinely nuanced (ADR 0005's "topology labels from config
are future work" — true for deployment-config labels, but a wire-level
admin API to set labels on a growth-added node now exists), leave the ADR
text alone and flag the nuance in the audit's PR body rather than rewriting
toward a verdict that isn't clean. (PR closing the 2026-08-10 audit; see
`docs/adr/{0002,0005,0006,0010,0017}` and
`crates/{animus-cql,animus-env,animusd}/CLAUDE.md`.)
