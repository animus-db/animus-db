# A relayed internal-only `ClientRequest` variant must be wrapped in `Forwarded` at *every* call site that sends it across the wire, not just handled correctly on receipt

**A relayed internal-only `ClientRequest` variant must be wrapped in
`Forwarded` at *every* call site that sends it across the wire, not just
handled correctly on receipt** — the receiving side's "refuse if sent
bare" gate exists precisely to reject exactly the mistake of sending it
unwrapped, so a caller that forgets the wrapper doesn't hang or corrupt
state, it fails **loudly and immediately** with the refusal's own error
message. Adding `ClientRequest::ForceSeal` (the DynamoDB Streams
disable-triggered final seal, round-3 sealer PR) initially called
`ClientCtx::relay(addr, ClientRequest::ForceSeal { .. })` directly instead
of `relay(addr, ClientRequest::Forwarded { request: Box::new(ForceSeal
{..}), .. })` — every unit test passed (they all happened to run on a
single node, where the *local* branch of `force_seal_tablet` never goes
through `relay` at all), and the gap was caught only by
`dynamo_streams.rs`'s existing `update_table_stream_enable_and_disable_
through_every_node` test, which specifically issues the disable from a
**non-leader** node and therefore exercises the forwarding branch. The
loud, specific error (`"...must be sent wrapped in Forwarded"`) made the
diagnosis immediate once a real multi-node path exercised it. General
rule: a new forwarded-command variant's own test coverage must include at
least one call from a node that is **not** the tablet's leader — a
same-node test suite can pass in full while every cross-node send is
broken, because the wrapping mistake only manifests on the wire, not
in-process. (`crates/animusd/src/lib.rs`, ADR 0042/0043 round-3 PR5,
2026-08-14.)
