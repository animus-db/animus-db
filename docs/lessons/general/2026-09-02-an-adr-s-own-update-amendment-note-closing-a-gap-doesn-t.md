# An ADR's own "Update"/"Amendment" note closing a gap doesn't retroactively fix every other place that gap is cited — the fix has to be applied at each citation site independently (D-01 stale-prose sweep, 2026-09-02)

Auditing `docs/roadmap.md`'s section 0 against the code (per-row, always
re-verify by grepping before touching prose — never trust the roadmap's own
characterization of "reality," it's exactly the kind of secondhand claim
that can itself drift) surfaced the same shape of bug twice: ADR 0045's
"phantom no-image records" limitation was already marked fixed by a
dated "Update" note earlier in the *same file* (its own §"Named
follow-ups" section, ~70 lines above), but the ADR's **Consequences**
section still listed it as an accepted, permanent limitation — nobody had
gone back to the second citation once the first was updated. Likewise ADR
0042's Context section still described the multi-consumer trim policy as
"deferred" long after §8/§9 built it and §16's follow-up list stopped
carrying it. **The general lesson**: when a design note documents a gap in
more than one place (a Context/motivation section *and* a Consequences/
follow-ups section is the classic split — one frames the problem, the
other tracks its resolution), closing the gap only in the tracking section
leaves the framing section stale, and a reader who lands on the framing
section first has no signal to look further. Grep the whole document (or
the whole file, for a claim that might be restated in prose elsewhere) for
every citation of a gap before declaring it closed, not just the section
whose job is to track resolutions — and when only one citation site can be
fixed in scope, leave a one-line pointer at the other rather than silence.
A second, smaller instance of the same root cause: `docs/roadmap.md`
itself carried a "backs the dashboard's sparklines" doc-comment claim
copied across four separate call sites in two files
(`animusd::lib.rs` ×3, `admin.rs` ×1) — one grep for the exact phrase
found all four, but a narrower grep scoped to just the file named in the
bug report would have missed three of them silently.
