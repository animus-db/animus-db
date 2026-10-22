# A liveness test with a generous budget can be passing at 90% of it: measure the margin, not the verdict

Issue #1362: `large_metadata_catch_up_stays_live` (ProdEnv, 12 s budget) failed
once in the merge queue with "node 2 snap=0 applied=0". Looping it locally
showed it never failed but also never finished in under ~9 s (debug build),
against a comment claiming "well under a second". Nothing was wedged; the
budget was simply almost fully consumed on every run, so one loaded CI box
tipped it over.

Root cause: the leader answered every `InstallSnapshotResp` that merely
repeated the offset it already tracked (a duplicate ack, produced by an
overlapping `AppendEntries`-reject or heartbeat re-offer of the same chunk)
with another resend of the same chunk, up to `SNAPSHOT_ACK_RESEND_CAP`. Each
resend drew its own ack, which drew the next resend: a ping-pong that shipped
every chunk ~9 times (3.5 MB snapshot, 55 chunks of 64 KiB, each JSON-encoded
as a number array). Fix: a duplicate ack for a chunk already shipped ships
nothing; real progress still ships the next chunk at once, and a lost chunk is
still retried by the heartbeat backoff.

Generalizable points:
- When a flaky liveness test is not reproducible, loop it with `--nocapture`
  and look at the *elapsed time of the passing runs*; a test that always takes
  75% of its budget is already failing in the field.
- Instrument per-message send/ack timestamps before theorising about
  connection state: the suspected transport reconnect/backoff paths were
  innocent, and the amplification was visible in 20 lines of `eprintln!`.
- A stale comment ("~1100 chunks", "well under a second") hid the regression:
  the chunk size had changed to 64 KiB and the image grown. Re-derive claimed
  numbers when touching a test.
- An ack handler that retransmits on *any* ack turns each duplicate into a new
  duplicate. Retransmit on loss evidence (timeout, regression), not on echo.
