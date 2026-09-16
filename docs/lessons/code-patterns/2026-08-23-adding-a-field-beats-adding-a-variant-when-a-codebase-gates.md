# Adding a field beats adding a variant when a codebase gates on variants (2026-08-23, ADR 0055).

**Adding a field beats adding a variant when a codebase gates on variants
(2026-08-23, ADR 0055).** Threading "is this read allowed to be stale"
through `ClientRequest` could have been three new variants
(`GetStale`/`ScanStale`/`KindScanStale`) or one `#[serde(default)] stale:
bool` on the three existing ones. The field won for a reason specific to
this repo: a new `ClientRequest` variant must be classified in ADR 0047's
exhaustive `surface_of` table and checked against every gating match site
(`is_relayable_command`, `cp_serve_forwarded`, admin filters) — and the
standing lesson about those is that a *missed* allowlist is a bimodal
per-process flake the compiler can't catch. A new **field**, by contrast,
makes `error[E0063]: missing field` enumerate every construction site for
you, and a `#[serde(default)]` keeps old peers decoding. General rule: when
the alternative is "the compiler finds every site" vs. "I grep for every
gate," take the compiler — and note that this is the same instinct behind
the `RoleAddrs` port-addition advice above, applied to an enum instead of a
struct.
