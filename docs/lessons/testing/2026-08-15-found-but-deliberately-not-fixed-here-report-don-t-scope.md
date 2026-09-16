# Found but deliberately not fixed here (report, don't scope-creep): `streams_e2e.rs::auto_split_mid_stream_with_live_consumer_across_every_ node` (D8) is flaky on `main` independent of any change in this PR

**Found but deliberately not fixed here (report, don't scope-creep):
`streams_e2e.rs::auto_split_mid_stream_with_live_consumer_across_every_
node` (D8) is flaky on `main` independent of any change in this
PR** — confirmed by running the *unmodified* file against the same
workload: an intermittent `exactly-once delivery` over-count (delivered
> expected by a handful of records) under `tiny_seal_knobs()`'s
size-1-triggered rapid resealing. A related, likely-connected symptom
found independently while building PR1's own e2e cell: under
*non-tiny* seal-byte knobs with a real write burst crossing the
threshold many times in quick succession, a handful of records can go
missing from *every* segment and the open tail alike (base row present
via `GetItem`, change record nowhere) — reproducible with no split
involved at all, so it isn't the split-basis bug this PR fixes. Both
point at a timing sensitivity in `change_consumer_loop`'s seal arm
(`animusd::index_drain`) under many-seals-in-quick-succession, not
investigated further here; PR1's own new e2e cell sidesteps it entirely
by using the **age** seal trigger (`seal_bytes` set high enough to never
fire) instead of the byte one, so each side seals exactly once. Worth a
dedicated investigation as its own PR.
