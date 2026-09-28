# A live soak's stall detector must track each op kind separately

**Context.** The ADR 0026 inbox series (#1057/#1065/#1071) and #1062 were validated with a live
`animusd --cluster-control 3 --cluster-data 5 --auto-split-bytes 1000000` soak. The load had two
parts: bulk `/admin/data/seed` chunks and random-key `PutItem`s. The driver was meant to capture
per-tablet Raft diagnostics once "no write succeeded for 2 minutes".

**What went wrong.** The detector reset its "last success" clock whenever *any* request
succeeded. At about 5 minutes, the bulk seed wedged on one tablet (#1064). Every seed chunk then
timed out at 60 s for the rest of the run. But single-key `PutItem`s kept succeeding in
milliseconds, because they happened to land on unaffected tablets. So the detector never fired.
The wedge was only found later, by reading the raw per-request timing log.

A related trap is reading a flat RSS curve as evidence that a leak is fixed. RSS also goes flat
when the workload stalls. So report RSS against rows written, not just against time.

**Rule.**
- A soak's liveness signal must be tracked **per op kind**, and ideally per target, such as the
  tablet or key range.
- A stall is "any op kind has made no progress for N", not "no op of any kind has made
  progress for N".
- Traffic that stays healthy on unaffected shards will mask a single wedged shard.
- Before calling a memory curve "flat", check that the workload was actually moving during the
  flat stretch.
</content>
</invoke>
<invoke name="Bash">
<parameter name="command">cd /home/user/animus-db && git add docs/lessons/testing/2026-09-28-a-live-soak-stall-detector-must-track-each-op-kind-separately.md && git commit -q -s -m "docs(lessons): a live soak's stall detector must track each op kind separately" && git log -1 --format=%B | grep -ciE "claude-session|co-authored"; git push -q origin claude/demux-inbox-cap 2>&1 | tail -1; git log --oneline -1