# A client-side view that needs to know "what am I" (this instance's own role/identity) must resolve that from a fast, self-only probe, kept structurally separate from any slower fan-out the same page also performs — never derive it from, or gate its resolution on, the fan-out itself.

**A client-side view that needs to know "what am I" (this instance's own
role/identity) must resolve that from a fast, self-only probe, kept
structurally separate from any slower fan-out the same page also
performs — never derive it from, or gate its resolution on, the fan-out
itself.** Gating the AnimusDB Console's tabs on the serving node's own role
(ADR 0035 PR7) could have derived that role by waiting for the existing
cluster-wide `/admin/peers`-seeded fan-out (`loadAll()`) to complete and
then finding "this node" in the results — but that fan-out's per-peer
fetches have no timeout and can each take as long as the slowest/most
unreachable OTHER node in the cluster, so gating first paint on it would
let one dead peer freeze the sidebar (and thus the whole page) on every
node, not just the one actually talking to that peer. The fix
(`dashboard_core.js`'s `loadSelf()`) fetches only `SEED`'s own endpoints —
never a peer — and `loadAll()` calls it first, before the slower fan-out,
so first paint and tab gating both resolve at local-fetch speed
regardless of peer health. **The dual lesson, from the same feature**:
before writing a NEW discovery probe for a client-side page, check
whether an EXISTING fan-out the page already performs contains the
answer. The "Open cluster console" link needed to find some other
reachable control/combined node — which sounds like it needs its own
peer-probing logic, but `loadAll()`'s existing cluster-wide fan-out
already fetches every peer's `/admin/config` (and thus its `role`) for
the other views, so the link just reads that same `STATE.nodes`
data — zero new requests. General check for either half: does this
page already have a self-only endpoint to lean on before joining a
slower cluster-wide operation, and does this page already fetch the
data a new discovery need is asking for, before writing a second path to
get it. (`crates/animusd/CLAUDE.md`'s dashboard section, ADR 0035 PR7.)
