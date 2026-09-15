# A retry loop over a Raft write must distinguish "never accepted, retry is free" from "accepted, unconfirmed" before resubmitting — the latter doubles outstanding work under exactly the conditions that caused the timeout.

**A retry loop over a Raft write must distinguish "never accepted, retry is
free" from "accepted, unconfirmed" before resubmitting — the latter doubles
outstanding work under exactly the conditions that caused the timeout.**
Diagnosing `--auto-split 2000` failures that looked like a runaway/election
storm, a live reproduction (isolated cluster, sustained bulk-seed under
load) showed every Raft term — control plane and every per-tablet CP group
— stayed flat the whole time; `commit_index` kept climbing well past
individual write attempts already reported as failed. So the writes weren't
stuck, just slower than the 10s client timeout (measured ~12-27ms fsyncs on
this host vs. sub-ms on real NVMe — a slow/virtualized disk under a growing
number of independent per-tablet Raft WALs). The admin bulk-seeder's retry
loop (`action_data_seed`) turned that slowness into a pile-up: on **any**
`cp_batch_write` error, including a bare confirm-timeout, it resubmitted the
same entries — but `ProposeResult::Accepted` only means appended to the
leader's local log, not committed, so a confirm-timeout after `Accepted`
almost always means "still committing," and resubmitting appends a
**second, fully duplicate** Raft entry for the same data on top of one that
was probably going to land anyway — safe by per-key LWW, but it doubles
fsync/replication load, compounding under the very slowness that caused the
timeout. Fixed by splitting propose from confirm
(`cp_batch_propose`/`poll_probe` in `animusd`) so a patient retry
(`cp_batch_write_patient`) can poll an already-accepted entry a second time
instead of re-proposing, while still proposing fresh on a genuine routing
failure (leader moved — e.g. a tablet split mid-seed, where `cp_route`
re-resolving on each attempt is exactly what's needed). General check for
any retry loop wrapping a Raft write: does a bare timeout distinguish
"definitely not accepted anywhere" from "accepted, just slow"? If not, a
slow/contended commit path gets a retry storm instead of patience.
**This recurred immediately in a sibling code path** (superseded by ADR
0028 — `auto_split_loop`'s `pending` map and `propose_split_data`/
`propose_and_confirm_split`/`cp_split_here` no longer exist; retained for
historical record) — worth treating as a
*pattern* to sweep for, not a one-off: `auto_split_loop`'s `pending` map
(the step-2 `propose_split` retry) has the identical shape, just already
half-fixed — `confirm_split` was already a poll-only primitive (propose and
confirm were never fused there the way `cp_batch_local` fused them), but the
retry loop still called `propose_split_data` (propose **and** confirm)
fresh on every ~2s tick regardless of whether the prior attempt reached
`Accepted`. `Split` apply is idempotent (a group splits once; re-application
is a no-op) so this was never a correctness bug, purely a wasted-work one —
same fsync/replication doubling, same live-repro signature (flat Raft terms,
`commit_index` still climbing). Fixed the same way: `propose_and_confirm_split`
takes a `confirm_rounds` count, and the pending-retry call (plus
`cp_split_here`, the cross-process counterpart, which can't tell if its
caller is about to retry) passes 2 instead of 1 — poll the already-accepted
entry a second time before the *next* tick would otherwise re-propose.
Lesson beyond the original one: when a retry-amplification bug is found and
fixed in one place, grep for the same *shape* (propose-then-poll, called
again from a loop on bare timeout) elsewhere in the same subsystem — it is
rarely truly a one-off.
