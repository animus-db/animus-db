# A sim CPU model that serves timers first hides timer lateness

ADR 0078's load monitor measures how late a `sleep(20 ms)` wakes. Under
`Simulator::set_cpu_cost` the monitor read zero lag through a 60 s, 1.25x
overload: the model runs expired timers ahead of waiting message deliveries (on
purpose; that is what lets heartbeats be late while election timers are not),
so the node's whole backlog sits in the delivery queue and a timer-based probe
never sees it. The collapse the monitor exists for is invisible to the probe.

What worked: keep the production probe task and test it where it is truthful (a
`pause`d node defers timers, so the probe fires late), and in the overload
cells feed the monitor from the model's own queue (`cpu_backlog` deliveries x
per-event cost). When a mechanism's input is a property the simulator does not
model, say which model the corpus uses instead, and test the real input
separately. Check a new signal's reading in the cell before building asserts on
it: `max_lag_ms` in the report would have shown zero on the first run.
