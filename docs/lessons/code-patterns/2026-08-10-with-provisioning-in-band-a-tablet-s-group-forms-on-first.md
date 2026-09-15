# With provisioning in band (a tablet's group forms on first access, not at startup), a node that *is* a replica of a not-yet-hosted tablet must WAIT, not forward.

**With provisioning in band (a tablet's group forms on first access, not at
startup), a node that *is* a replica of a not-yet-hosted tablet must WAIT, not
forward.** Routing's "I host no replica → forward to any route" fallback misfires
during the formation window when a replica-to-be hasn't stood its group up yet — it
forwards to a node that doesn't host the leader → "forwarded CP op: not the leader
here". Gate the forward on "this node is **not** in the tablet's replica set"; a
replica waits for its own election. And **don't paper over formation latency with a
synchronous serve-wait on the provisioning path** — it made the first write block on
full formation (regressing a restart test); `cp_route` already waits, so provisioning
returns once the tablet is in `Metadata`. (ADR 0023, `animusd` `resolve_cp_route`.)
