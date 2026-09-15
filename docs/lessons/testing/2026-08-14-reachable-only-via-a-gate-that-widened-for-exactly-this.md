# "Reachable only via a gate that widened for exactly this case" needs an end-to-end test, not just a component-level one — the gate and the function it feeds can each look locally correct while their *composition* drops the very case the gate was widened for.

**"Reachable only via a gate that widened for exactly this case" needs an
end-to-end test, not just a component-level one — the gate and the
function it feeds can each look locally correct while their *composition*
drops the very case the gate was widened for.** Building the DynamoDB
Streams sealer's hot-trim rework (F10/F12-b), the per-tablet loop's outer
gate (`gsis.is_empty() && !stream_enabled`, `index_drain.rs`) skips a
tablet once its stream disables and it has no GSI — correct for a table
that *never* streamed, wrong for one that just finished a disable's final
seal: skipping it forever means the hot-trim arm never runs again to
actually delete the now-fully-sealed hot tail, whose correctness had been
silently depending on a *race* (the periodic loop happening to tick, with
the schema not yet flipped, in the narrow window between the final seal's
own commit and `SetTableStream{None}`'s). One test
(`disabled_draining_stream_does_not_block_trim`, 2 writes) passed reliably
because that race happened to resolve in its favor every run; a materially
identical second test (`disable_final_seal_then_reenable_continues_the_
epoch_chain`, 3 writes) reproducibly timed out, because the tiny
extra work shifted the race the other way. Neither `trim_janitor` in
isolation (its own unit-shaped tests all passed — "no expected term ⇒
block" was internally consistent) nor the outer gate in isolation looked
wrong; only running the *disable-then-verify-convergence* sequence
end-to-end, twice, with slightly different timing, exposed that the gate
needed widening (`ever_streamed`, keep visiting a tablet that has ever
sealed) **and** `trim_janitor`'s own "no expected term" branch needed to
flip from "block" to "trim unconditionally" (the two fixes are a pair —
widening the gate alone would have reached the old "block" branch and
changed nothing). General rule: when a background loop's own top-level
gate decides "does this item still matter to me," and a later lifecycle
event (disable, drop, expire) can make the answer flip from yes to no,
write the test that drives *through* that transition and polls for the
eventual-consistency property on the other side — a gate widened for a
new terminal state, paired with a function whose fallback branch was
never re-examined for that same state, is exactly the shape that passes
every unit test and flakes (or silently stalls) in integration.
(`crates/animusd/src/index_drain.rs`, ADR 0042/0043 round-3 PR5,
2026-08-14.)
