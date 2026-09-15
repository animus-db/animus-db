# A fault enabled before a multi-step setup sequence finishes can corrupt the setup itself, not just the operation under test (ADR 0069, S-03 PR 2)

Building `crates/animus-test/tests/segment_store_encrypted_fault_corpus.rs`'s
`round_trip_survives_put_ack_lost` cell, the first draft configured
`SegmentFaultConfig::set_put_ack_lost_prob(1.0)` on the raw
`SimSegmentStore` *before* calling `EncryptedSegmentStore::open` — but
`open` itself performs a `put` (sealing the marker object,
`verify_or_init_segment_store_marker`'s "fresh store, key given" branch),
so every single run failed inside `open` itself, before the test ever
reached the `put` it actually meant to fault-inject. The fix: enable the
fault only after the multi-step setup (`open`) has genuinely finished,
and give the setup step's own fault interaction (a marker-put ack-lost,
recovered by retrying `open`) its own dedicated cell instead of letting it
accidentally dominate an unrelated one.

**General form**: before enabling an ambient fault for "the operation
under test," check whether anything upstream of that operation — a
constructor, an `open`, an initialization step — goes through the same
fault-injected seam. A fault config that's "on" too early doesn't produce
a wrong answer about the intended property; it silently tests a
completely different (and less interesting) one, namely "does setup
itself tolerate this fault" — which may be worth its own cell, but is
never a substitute for the property the test's name promises.
