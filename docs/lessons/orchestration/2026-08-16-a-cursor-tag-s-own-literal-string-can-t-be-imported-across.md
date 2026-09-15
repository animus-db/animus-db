# A cursor tag's own literal string can't be imported across the same one-way dependency, and re-declaring it is the honest option, not a smell

**A cursor tag's own literal string can't be imported across the same
one-way dependency, and re-declaring it is the honest option, not a
smell** (same delivery, 2026-08-16). `animus-cp-data::cursor::
classify_tag` needs to compare against `"gsi"`/`"backfill:"`, but the
canonical constants (`animusd::index_drain::GSI_TAG`, `backfill_tag`)
live in a crate one layer *above* it in the dependency graph — the same
direction constraint as the lesson above. Restating the literals as named
constants with a doc comment pointing at the upstream source they must
stay byte-identical to (rather than either an inline bare literal with no
such pointer, or contorting the dependency graph to share them) keeps the
duplication visible and intentional instead of accidental — the
regression test that checks the *result* of the classification is the
real safety net; the doc pointer is what tells the next person touching
either side that the two need to move together.
