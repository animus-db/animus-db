# A cluster bring-up barrier must gate on members-*Active*, not members-*present*, or a still-`Down` node is skipped by placement

**A cluster bring-up barrier that gates on "some control leader + a non-empty
members map" is too weak whenever the first thing the test does is
placement-sensitive.** A node's own `RegisterNode` self-registration can land
as `Down` (the status a freshly-registered member carries until the failure
detector promotes it) *before* `bootstrap`'s idempotent `Active` upsert
applies — so a "members present" barrier returns while one or more founding
nodes are still recorded `Down`.

That is load-bearing because placement is **status-sensitive**:
`animusd::ClientCtx::provision_tablet` seeds a table's first tablet from the
first `MAX_REPLICATION_FACTOR` members whose `status == Active`, **in id
order**. A founding node still `Down` at the instant the first write
provisions the tablet is silently skipped in favour of a higher-id `Active`
peer, and stays skipped until the detector promotes it (~100-200ms later,
wider under CPU load). So a placement-sensitive first write can land on the
wrong node set — e.g. an RF-3 tablet on `{n0,n1,n3}` instead of `{n0,n1,n2}` —
which flakes any test that hard-codes which node is a voter vs. the idle
spare. This was issue #1028, where
`learner_reconfigure.rs::spare_replacement_passes_through_an_observable_learner_state_and_keeps_serving`
hard-coded `spare = raftkv_ids[3]`; when the tablet formed on `{0,1,3}`, id 3
was a real voter and the assertion that it passed through the `learners` set
failed.

**The barrier must gate on all founding members being `Active`.** "All
Active" is a strict strengthening of "all present" (it still requires a
leader, and Active implies present), and once every member is promoted the
detector has caught up, so a subsequent `provision_tablet` sees the full
`Active` set and places deterministically. A placement-sensitive first write
must additionally be preceded by an explicit "every id I care about is
`Active` in the control leader's metadata" wait — and any downstream
assumption about *which* node is the spare must be **derived from the formed
voter set**, never hard-coded to an id, because even an all-`Active` set can
be placed as any RF-sized subset in id order.

**Two secondary lessons from the same fix.** (1) A test that kills "a
non-leader replica" to trigger a reconfigure must kill a member of the
**actual formed voter set** that is not the leader, not merely a non-leader
*index* — killing a node that was never a voter triggers nothing. (2) This
weak-`await_bootstrap` helper had been copy-pasted into ~60 `animusd` test
files, so the fix belongs in a shared `support::` barrier
(`support::await_bootstrap` over `&[Node]`, all-members-Active), migrated into
each copy, so the same race cannot bite them one at a time.
