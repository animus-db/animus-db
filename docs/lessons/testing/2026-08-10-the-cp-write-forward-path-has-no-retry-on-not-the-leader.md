# The CP write-forward path has no retry-on-"not the leader here," unlike the read path — a test that forwards to a group which hasn't finished hosting/electing yet must retry client-side, or it flakes on a real, pre-existing timing window, not a bug the test introduced.

**The CP write-forward path has no retry-on-"not the leader here," unlike
the read path — a test that forwards to a group which hasn't finished
hosting/electing yet must retry client-side, or it flakes on a real,
pre-existing timing window, not a bug the test introduced.**
`ClientCtx::cp_read`/`cp_scan_one` retry internally on the `"; retry"`-class
error shape (`read_should_retry`), but `cp_write`'s `Forward` branch
(`ClientCtx::cp_write`) does not — it returns whatever the forwarded node's
`cp_serve_forwarded` answers verbatim, including a clean, non-`"; retry"`
"not the leader here" if the forward lands before the receiving node's own
tablet-host reconciler has stood the freshly-provisioned tablet's group up
and elected. `cp_route`'s own doc says the client is expected to retry with
fresh routing on exactly this shape, so it is a documented contract, not an
oversight — but it means a **first write right after provisioning a fresh
table**, forwarded to a node whose reconciler hasn't caught up yet, can
legitimately fail once. The window is usually sub-millisecond in combined
mode (the reconciler reacts to an event-driven `metadata_watch` wake on the
same node that just committed the tablet), but widens to the reconciler's
500ms fallback-poll interval on any node reached only through a mirrored
`Metadata` view (an ADR 0030 growth node, or an ADR 0035 control-only
node's forward target) — exactly the shape a control-only-cluster
integration test is likely to exercise for the first time. Write such a
test's first-write assertion as a bounded retry poll (`loop { put; if ok
return; sleep; }` under a `timeout`), not a bare one-shot assert — the same
"converged-or-timeout, not a fixed one-shot" discipline this file already
documents for eventual properties, just showing up on the write path this
time. (ADR 0035 PR3 `tests/control_only.rs`'s mixed-cluster test, caught on
the very first run.)
