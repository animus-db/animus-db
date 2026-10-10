# A `sleep(POLL)` in a wait loop is a latency floor, not a granularity

Issue #1197: a `ConsistentRead: true` GetItem never finished under ~21 ms on a
3-node cluster while an eventual read took ~1 ms. `RaftKvNode::read_barrier`
(and `ensure_ceiling_above`) waited for "quorum acked the ReadProbe" and
"engine applied the ReadIndex" with `loop { check; env.sleep(READ_POLL) }`
(`READ_POLL` = 20 ms). The acks land in about a millisecond, but the loop only
looked again at its next 20 ms boundary, so every barrier cost one full poll
tick no matter how fast the network was. Every read paid it; the sleep was
never the *safety* mechanism, just the only wake source.

Rules that fall out of it:

- A poll interval in a wait loop is a **minimum latency for the common case**,
  not a bound on the worst case. If the thing you wait for has an in-process
  producer (an ack handler, the apply task), give it a wake and keep the sleep
  only as a safety net for transitions that raise none.
- The primitive was already in the file: `AppliedWatch` is a multi-waiter,
  executor-agnostic watermark (many reads wait at once, so a lone `AtomicWaker`
  would lose all but one). Used as a generation counter it covers "an ack
  landed" too. Prefer reusing it over inventing another signal.
- **Sample the marks before evaluating the condition**, then park on
  `changed(mark)`: the watch resolves immediately if it already moved, so a
  wake between the check and the park is never lost.
- Nothing fails when such a floor exists: the read is correct, just slow. Guard
  it with a `SimEnv` test that measures the *virtual* completion time against a
  budget well under the poll interval, on a network with real (nonzero) latency
  and with one peer cut off (`tests/it/read_index_latency.rs`). With
  near-zero-latency sims a poll floor hides, because the first poll already
  sees everything.
- The timeout/step-down checks are the other half of such a loop; they still
  ride the safety poll, which is why it stays.
