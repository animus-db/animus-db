# A new mode of an existing spec field reaches every consumer, including the dashboard JS

When G-d added the MREC mode to `GlobalTableSpec` (the field G-c's MRSC code
already read), the Rust consumers were found by the `is_mrsc()` audit, but the
dashboard's `globalSpecOf` simply returned "the `global` spec if present" and
then compared a leader's Region to `preferred_leader_region`. For an MREC table
that field is empty, so every leader would have shown as "off preferred". No Rust
gate or test sees this; only reading the JS did.

Rule: when a spec/enum gains a mode, grep **every** reader of the field across
languages (`rg "\.global\b|globalSpecOf|preferred_leader"` over `*.js`,
`website/`, CLI), not just the typed match sites the compiler forces. Give the
new mode its own accessor (here `mrecReplicaText`) and make the old accessor
return nothing for it.

Also from M6: a real-process two-cluster test that reuses an existing TLS PKI
helper (`mrec_peer_transport.rs`) was cheap (about 3 s) because the simulation
had already exercised every protocol corner; spend the real-socket budget on one
end-to-end happy path plus the TLS refusals, and leave faults to the sim.
