# A new `ClientRequest` variant that can be *forwarded* must be handled in BOTH the main serve loop AND `cp_serve_forwarded` — a single-node test can't catch the missing half.

**A new `ClientRequest` variant that can be *forwarded* must be handled in BOTH
the main serve loop AND `cp_serve_forwarded` — a single-node test can't catch the
missing half.** `animusd` CP ops route locally or **forward one hop** to the
leader's node wrapped in `ClientRequest::Forwarded`; the receiver dispatches the
inner request through `cp_serve_forwarded`, a *separate* match from the top-level
serve loop. A batch (`PutBatch`) added only to the serve loop works whenever the
connected node happens to host the tablet leader and silently errors ("unexpected
forwarded request") when it must forward — the same bimodal per-process failure
shape as the `is_relayable_command` allowlist gap. When adding a forwardable
variant, grep for the request enum's name across *both* match sites and add the
arm to each; regression-test it through a **follower/non-leader-connected** node
in a per-process cluster. (`animusd` `cp_serve_forwarded`; batch put, ADR 0017.)
