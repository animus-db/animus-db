# A fake that mirrors a hand-typed literal instead of the producer's own output makes a whole test suite self-consistently wrong — pin fakes to the real serializer's literal, and cross-check any two vocabularies that look alike (S-07d, `GET /admin/config` `role`)

S-07d's growth loop gated "has the promoted pod restarted into a
control-capable role?" on `GET /admin/config`'s `role` field equalling
`"both"`. That literal is real — but it is `desired::cluster_config::
NodeRole::Both`, the generated `cluster.json`'s per-ordinal dispatch
vocabulary, a different document produced by this operator. `animusd`'s
own admin view emits `"control"`/`"data"`/`"combined"` (pinned by
`crates/animusd/tests/dashboard_endpoint.rs`). The comparison could never
match, so `member/add` was never issued and every `kind` e2e leg timed
out waiting for the fourth voter — while all 190+ operator unit tests
passed, because `crate::fakes::FakeAdminClient`'s `/admin/config` stub
had been written from the same wrong assumption. A fake that agrees with
the code under test proves nothing about the producer it stands in for.

**General form**: when a consumer matches on a literal another component
emits, (1) copy the literal from the producer's serializer or the
producer's own pinned test, never retype it from memory or from a
similarly-named enum in the consumer's crate; (2) add one consumer test
that uses an independent double carrying the producer's exact literal,
separate from the shared fake, so the fake and the matcher cannot drift
together; (3) when two vocabularies describe the same concept
(`NodeRole::Both` vs `role: "combined"`), document the pair at the
match site. The tell that this class of bug is present: unit suite green,
integration/e2e stalls with "waiting for X" forever and no error, because
the gate is a silent `false`, not a failure.
