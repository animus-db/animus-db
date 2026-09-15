# The token prefix is a *wire-edge/seeder* convention, not a storage invariant — a transform that renders/parses a stored key must detect the layout by content, not assume it.

**The token prefix is a *wire-edge/seeder* convention, not a storage invariant —
a transform that renders/parses a stored key must detect the layout by content,
not assume it.** The ADR 0022 `token || escape(pk) || rk` layout is built *above*
`cp_write`; the DynamoDB/CQL edges and the bulk seeder add the token, but the
plain-client `Put` stores its key **verbatim** (`cp_put_local`, un-prefixed). So a
dashboard key view that hex-formats "the first `TOKEN_BYTES` bytes as the token"
mangles a plain key (`admin-key` → `61646d696e2d6b65:y`). Gate the split on the
leading run actually being **non-printable** (a Murmur3 token almost always has a
non-printable byte; a printable key is shown as text) so both key populations
render correctly. Same "keys aren't uniform below the edge" root as the seed-key
entry above. (`animusd` `admin.rs::key_display`/`parse_key_display`; the
`admin_endpoint` test writes a *plain-client* `admin-key` — it caught exactly this.)
