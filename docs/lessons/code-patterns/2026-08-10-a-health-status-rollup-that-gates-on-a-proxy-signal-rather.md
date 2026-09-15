# A health/status rollup that gates on a *proxy* signal rather than the actual risk that signal stands in for can diverge from reality forever, because the two clear on different triggers. General check for any rollup built from "X is down/unhealthy ⇒ overall is unhealthy": does the thing being protected (data replication, request-serving capacity) actually recover on a faster/different path than the raw signal does — and if so, gate on the protected property, not the signal.

**A health/status rollup that gates on a *proxy* signal rather than the
actual risk that signal stands in for can diverge from reality forever,
because the two clear on different triggers. General check for any rollup
built from "X is down/unhealthy ⇒ overall is unhealthy": does the thing
being protected (data replication, request-serving capacity) actually
recover on a faster/different path than the raw signal does — and if so,
gate on the protected property, not the signal.** (Original mechanism —
gating on a lingering `Down` member instead of per-tablet status —
superseded by the "health ≈ is the data at risk" ladder
(`quorum-lost`/`under-replicated`/`healthy`/`forming`); full writeup moved
to `docs/engineering-lessons-archive.md`.)
