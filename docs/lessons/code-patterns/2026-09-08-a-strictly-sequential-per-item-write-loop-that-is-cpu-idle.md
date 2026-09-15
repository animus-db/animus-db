# A strictly-sequential per-item write loop that is CPU-idle (it's waiting, not computing) is a pipelining opportunity, not a correctness boundary that has to move to fix it (2026-09-08, the admin seeder's images arm).

**A strictly-sequential per-item write loop that is CPU-idle (it's
waiting, not computing) is a pipelining opportunity, not a correctness
boundary that has to move to fix it (2026-09-08, the admin seeder's
images arm).** `admin::action_data_seed`'s images-carrying-table arm
(any table with a Stream/GSI/LSI/PITR) issued one `cp_kind_write_item`
call at a time, fully `.await`ed before starting the next — modeled,
per its own comment, on "correctness over throughput for that rare seed
target." Measured on a real 3-node cluster: ~140 keys/s (~7ms/row), and
the 7ms is almost entirely the confirm-poll round-up + a possible
forward hop + one fsync, not CPU — a fully sequential loop pays that
latency once per row for no reason, since every row has a distinct key
and each key's own evaluate-at-leader apply (ADR 0054) is independent of
every other key's. Fixed by pipelining the arm with bounded concurrency
(`futures::stream::iter(rows.iter().map(Ok)).try_for_each_concurrent
(SEED_IMAGES_CONCURRENCY, |row| async { ctx.cp_kind_write_item(..).await
})`, `SEED_IMAGES_CONCURRENCY = 32`) — no `tokio::spawn`/`env.spawn_task`
involved, the combinator polls every in-flight future from within the
same request task, so nothing about the `Env` seam's spawn discipline is
touched. Measured after the fix: 2000 rows into a Stream-enabled table
in ~3.5s (~570 keys/s), a >4x win with zero change to the per-row
correctness contract (`try_for_each_concurrent` still stops issuing new
rows and surfaces the first error as the chunk's own error, exactly like
the old loop did, so the outer whole-chunk retry loop needed no change).
**The general lesson**: before assuming a sequential `for req in reqs {
... .await }` loop over independent keys is load-bearing for
correctness, check *why* it's sequential — if the answer is "each
iteration is dominated by a network/consensus round trip against a
key-independent write path," bounded concurrency is very likely safe and
is worth measuring before reaching for a bigger structural change (e.g.
moving to the marker/fast-arm path, which would trade away the very
correctness property — per-item evaluation — the images arm exists for).
Pin the concurrency bound with a comment stating what it's buying (how
many round trips it overlaps) and what it's protecting against (the
leader's own in-flight write set staying bounded), the same way this
crate already documents `SEED_BATCH_SIZE`/`SEED_WRITE_ATTEMPTS`.
Regression: none added here (this is an admin dev-tooling endpoint with
no existing latency-floor test, unlike `write_path::kind_eval_confirm_
backoff_tests` above, which *is* the load-bearing test for the confirm
loop this fix rides on) — see `crates/animusd/src/admin.rs`'s own
`SEED_IMAGES_CONCURRENCY` doc for the constant's sizing rationale.
