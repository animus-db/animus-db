# A negative control only bites if the workload makes the disabled mechanism matter

Building the `sim_world_mrec_corpus` (G-d M5) the first run of two controls
passed silently ("the oracle did not notice the disabled mechanism"), and the
oracle was fine: the workload never exercised the mechanism.

1. **Last-writer-wins by arrival** (apply stale versions). Writes were
   issued back to back with no shipping in between and the shippers ran in a
   fixed region order, so the newest write always shipped first and every
   stale value arrived *before* anything it could clobber. Fix: a "round"
   = one write per region on one key **before** any shipper step, then a
   step, with a *fresh key per round* (a single hot key plus a monotonic
   "floor" oracle only ever checks the last round: earlier rounds are
   dominated by it).
2. **Cursor advances before the ack.** Later writes after the heal re-dirtied
   the lost keys and re-shipped them, masking the loss. Fix: nothing is
   written after the partition heals (quiescence heals), so a row shipped
   into the partition is never re-sent by anything else.
3. **Loop prevention.** Counting shipped rows was too noisy to calibrate a
   bound that bites; a spy on the receiving side of the WAN that asserts
   "the record's stamp names its sender" is exact.

Rules: write the control first and watch it fail for the *named* reason (the
panic text carries an `ORACLE-<name>` tag the control asserts on) before
trusting the positive cell; give every fault cell an `expect` that proves the
fault happened (a resync metric, a crash count, two active tablets), or the
cell is green for the wrong reason.

Also: an `assert!` inside a future handed to `SimWorld::drive` panics the
*task*, which the simulator swallows, so the symptom is "did not finish" with
no message. Return a `Result` out of the future and panic on the test thread
(this hid a non-200 `GetShardIterator` as a 120 s "hang").
