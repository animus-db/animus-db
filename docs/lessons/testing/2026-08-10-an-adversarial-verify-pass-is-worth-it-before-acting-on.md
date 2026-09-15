# An adversarial-verify pass is worth it before acting on audit/review findings — and re-verify against the branch you'll edit.

**An adversarial-verify pass is worth it before acting on audit/review findings —
and re-verify against the branch you'll edit.** Of the audit's 6 highest-stakes
claims all 6 confirmed, but two materially changed shape under verification (the
storage flush bug's trigger is admin flush/compact, not client writes; the
~15/s seed throughput was primarily the 50ms confirm-poll cap, not the election
storm) — and several perf findings were already fixed on `main`, which had moved
past the audited checkout (pre-vote, single-write-latency, cp-batch-put). A
finding is (claim × trigger path × branch); verify all three.
