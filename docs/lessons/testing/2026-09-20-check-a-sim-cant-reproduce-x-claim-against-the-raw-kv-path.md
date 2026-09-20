# A "the wire-based sim scenarios can't reproduce X" note recorded for the DynamoDB path needs rechecking against the fixture's raw-KV path before it's trusted (2026-09-20, ADR 0061 rung O, C-15)

**What happened.** `crates/animusd/CLAUDE.md`'s D4-PR-2 disposition for
`tests/cp_plane.rs` kept `tablet_auto_splits_on_bytes_with_skewed_value_
sizes` real-socket with the stated reason that its byte-weighted-median
balance claim "isn't reproduced by the new `SimCluster` scenarios, which
don't correlate a DynamoDB item's `pk` to its hashed token range" — true
as far as it went, because `sim_cluster_auto_split.rs` drives that
property entirely over the DynamoDB wire (`PutItem`/`GetItem`). ADR 0061
rung O's investigation (issue #997) re-checked two sibling tests in the
same file against `SimCluster::put_raw`/`raw_get` instead — the
fixture's literal, un-encoded byte-key path, built specifically because
`ClientCtx::trigger_split`'s `split_key: Vec<u8>` compares raw stored-key
bytes with no decoding step — and found that path makes exactly the
correlation the DynamoDB-wire path can't: a written key's post-split
range membership is a trivial `range.contains(&key)` check on literal
bytes, the same idiom `sim_cluster_admin.rs::run_raftkv_key_count_is_
scoped_per_tablet_after_split` already uses. `cp_tablet_splits_and_both_
halves_serve` converted cleanly on this path; a similar raw-KV auto-split
scenario for the byte-weighted-median claim was independently plausible
enough to warrant its own bounded spike rather than accepting the
DynamoDB-scoped "can't reproduce" note as a reason to leave the sibling
test real-socket forever.

**The same investigation separately confirmed a related but distinct
gap has no raw-KV workaround.** `cp_member_addresses_register_and_
replicate`'s original assertion — that a member's registered address
"parses as a `std::net::SocketAddr`" — genuinely has no sim equivalent on
*any* path, wire or raw-KV: `SimCluster::seed_members` registers bare
`NodeId` strings (`"0"`, `"1"`, …) as `ClusterEdgeState`'s own routing
keys, not real socket addresses, so `"0".parse::<SocketAddr>()` fails
regardless of the fixture's KV-encoding choice. The fix there wasn't a
different sim path — it was converting to the actual property under
test (replicated presence/identity of the address entry across every
node), which the incidental "happens to be a real address in production"
fact was never really testing on its own.

**What to do.**
- Before accepting a recorded "the sim scenarios can't reproduce X"
  reason as still current, check whether it was scoped to one dispatch
  path (commonly the DynamoDB wire, since that's usually built first) or
  is a structural fact about the fixture itself. `SimCluster::put_raw`/
  `raw_get`/`create_table` write/read literal, un-encoded byte keys
  specifically for split-key semantics — a property that's awkward to
  state over DynamoDB's Murmur3-hashed item keys can still be trivial
  over the raw-KV path several `sim_cluster_admin*.rs` scenarios already
  use.
- Distinguish the two failure modes before writing either "can convert"
  or "permanent": a claim scoped to the wrong dispatch path is worth a
  bounded spike on the other path before trusting it; a claim about an
  incidental production fact (an address happens to be `SocketAddr`-
  shaped) with no sim analog on *any* path should be converted to the
  real property under test, not left as a permanent residual waiting for
  a fixture capability that was never actually missing.
