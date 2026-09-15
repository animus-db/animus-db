# A regression's own split key can quietly exempt it from the bug it claims to cover — check whether the fixture's boundary is realistic, not just whether the test passes.

**A regression's own split key can quietly exempt it from the bug it
claims to cover — check whether the fixture's boundary is realistic, not
just whether the test passes.** Issue #355 suspected (from code reading)
that a split's right child's `"gsi"` cursor key (`animusd::index_drain`'s
`drain_tablet`, `cursor::cursor_key(&group.scope_range().start,
GSI_TAG)`) is token-truncated below the child's own `range.start`,
routing the watermark write to the LEFT sibling instead — confirmed true
by direct reproduction (`index_drain.rs::gsi_drain_cursor_tests::
split_right_childs_gsi_cursor_after_a_non_token_aligned_split_issue_355`,
`#[ignore]`d, expected to fail until fixed): the watermark stayed `None`
and the change log stayed pinned at 8 records across 20 drain ticks
(~8s), and the cursor row for the right child's own token was found
physically present on the LEFT sibling's engine, absent on the right's
own. The existing sibling regression
(`split_right_childs_cold_start_re_reconciles_from_zero_without_
corrupting_the_gsi`) asserts the *opposite* — the cursor genuinely
advances — and is not wrong; it splits at `BOUNDARY`, a bare
`TOKEN_BYTES`-long numeric constant, which is *itself* already
token-aligned by construction. A real split's key
(`byte_weighted_median`, chosen from actual row content) is essentially
never that short, so the existing test's green result says nothing about
the shape production splits actually produce — it happens to sidestep
the exact precondition (`range.start` longer than `TOKEN_BYTES`) the bug
needs. The general rule: when a regression's fixture picks a boundary/key/
input by a rule visibly simpler than what production uses to pick the
same thing (a round numeric constant standing in for a real row's byte
string, a fixed-length id standing in for a variable-length one), treat
its passing as evidence about *that specific shape* only — check whether
the simplification also strips out the precondition the suspected bug
needs before trusting the green run as a disproof. GSI *correctness*
(every item still queryable) was unaffected either way — only the
cursor/trim bookkeeping breaks, matching `drain_tablet`'s own comment
that a watermark stuck at `None` means "reconcile everything, always
correct, just not incremental" (a liveness/efficiency defect, not a data
correctness one) — the same shape `advance_backfill_cursor`'s own doc
documents for the analogous, already-fixed backfill-cursor bug, though
the mechanism here is different: the backfill cursor used to be *locally
rejected* by a range fence (fixed by writing unfenced, directly on the
known-leader `group` handle); the `"gsi"` cursor write goes through
`ClientCtx::cp_kind_write_raw`, which **routes by the write's own key**
(`cp_route(table, &first_write_key)`) — so a token-truncated cursor key
doesn't get rejected at all, it gets *misrouted and successfully applied
on whichever tablet's declared range the truncated key actually falls
in*, which is silently indistinguishable from success unless you read
the raw physical key back off the sibling's own engine
(`CpGroup::local_get_kind` — bound-free, reads any key physically present
regardless of the tablet's declared range) rather than inferring the
landing site from the key-construction code alone.

**Fixed (2026-08-23).** `animus_cp_data::cursor::cursor_key` now embeds
the tablet's own `range.start` **verbatim** (never truncated), with a
trailing 2-byte big-endian length so `parse_cursor_key` can still recover
the tag unambiguously without relying on a fixed byte offset (the old
scheme's whole reason to truncate in the first place). The key is now
`range_start` extended with more bytes, so it always compares
lexicographically `>=` its own tablet's `range.start` by construction —
the routing half of `KeyRange::contains` is satisfied unconditionally,
not by luck of the split boundary's own byte content. The repro test
(`gsi_drain_cursor_tests::split_right_childs_gsi_cursor_after_a_non_
token_aligned_split_issue_355`) now asserts the fixed end-state
precisely — watermark advances, change log trims to zero, and the
physical cursor row lives on the right child's own engine and is absent
from the left sibling's — rather than only "eventually not `None`."
**Pre-fix stray rows**: a cluster that hit this bug before the fix could
have a truncated cursor row physically sitting on a sibling tablet's
engine forever, silently depressing that sibling's own `"gsi"`
min-over-rows watermark (redundant re-reconciliation there, never a
correctness or non-convergence issue — the same "reconcile everything,
always correct, just not incremental" property the bug's `None` case
already had). No migration/cleanup was written for it — the root
`CLAUDE.md`'s no-back-compat policy means clusters are recreated from
scratch, and a convergent sweep to find and delete such rows would need
to distinguish a truncated stray from a genuine (if unlikely) same-prefix
collision, which isn't a cheap, obviously-safe addition, so it was left
alone per the same policy rather than over-engineered.
