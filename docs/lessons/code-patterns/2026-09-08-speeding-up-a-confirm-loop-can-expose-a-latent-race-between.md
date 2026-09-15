# Speeding up a confirm loop can expose a LATENT race between quiescence's "skip forever once quiesced" optimization and a periodic (non-event-driven) sweeper — found landing the fix immediately above (2026-09-08).

**Speeding up a confirm loop can expose a LATENT race between
quiescence's "skip forever once quiesced" optimization and a periodic
(non-event-driven) sweeper — found landing the fix immediately above
(2026-09-08).** `auto_split_loop` sleeps a fixed `AUTO_SPLIT_INTERVAL`
(2s) between ticks and skips a tablet outright once `leader.
is_quiesced()` is true, trusting that "whatever this tablet's last
pre-quiescence tick already checked still holds" (ADR 0044 phase-1
PR6's own doc). That trust is sound only if the loop got at least ONE
tick while the tablet was genuinely non-quiesced — i.e. only if
`quiesce_after` (how long a tablet must be idle before it re-quiesces)
is comfortably *longer* than `AUTO_SPLIT_INTERVAL`. Production's own
default (`--quiesce-after`, 5s) safely clears `AUTO_SPLIT_INTERVAL`
(2s), but `index_drain.rs`'s own
`a_rewoken_tablet_is_picked_back_up_by_every_sweeper_within_one_
interval` test configured `quiesce_after` to a *test-convenience* 300ms
— far below 2s — purely so its own "an idle table quiesces" negative
control would resolve quickly. That mismatch was invisible for as long
as `cp_kind_eval_local`'s own write-confirm loop paid the 50ms flat-poll
floor above: a 40-item write burst then took ~2s of real time (`40 ×
50ms`), which reliably straddled at least one `AUTO_SPLIT_INTERVAL`
tick before the tablet could re-quiesce. The moment that floor was
fixed, the identical 40-item burst completed in well under 300ms,
letting the tablet re-quiesce *before* `auto_split_loop`'s next 2s tick
ever observed it non-quiesced — the loop then skipped it forever (no
further writes ever arrived to re-wake it), and the test timed out
deterministically, every run. **The lesson generalizes past this one
test**: a fixture that sets a "how long until X becomes idle" knob
shorter than the period of a periodic (not event-driven) background
loop that is supposed to observe X while active is not actually testing
what its own doc claims — it was passing by accident of unrelated
timing elsewhere, and any change that legitimately speeds up the
activity being observed can silently break it. Fixed by raising this
one test's `quiesce_after` to 3s (safely above `AUTO_SPLIT_INTERVAL`),
with a comment stating the invariant explicitly so a future reader
doesn't reintroduce it. When touching *any* loop whose speed a test's
own quiescence/timeout knobs were implicitly calibrated against, grep
for other fixtures using the same short-quiesce-plus-periodic-sweeper
shape before assuming "the assertions still pass" is the whole story —
a green run can still be resting on a timing coincidence the change
just removed.
