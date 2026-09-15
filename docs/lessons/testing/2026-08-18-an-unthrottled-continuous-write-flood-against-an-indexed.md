# An unthrottled continuous write flood against an INDEXED table racing a background convergence process can make that process livelock rather than exercise it

**An unthrottled continuous write flood against an INDEXED table racing a
background convergence process can make that process livelock rather than
exercise it** (issue #288). A first draft of the freeze-window regression
ran 6 concurrent max-speed `PutItem` loops (fresh TCP connection per
request, no pacing) against a table with a GSI, from just before a
split's kickoff until cutover — and the split never converged within a
90s budget, because split cutover itself gates on the GSI drain's own
veto (it must catch up to the max pending change record before a parent
can retire, `docs/streams-notes.md`), and the flood was generating new
change-log backlog faster than the drain could clear it — a genuine
live-lock, not a hang. It also incidentally triggered a real engine-level
I/O error on the sandbox under that load. Pacing the flood down to 2
lanes with a 20ms delay between attempts fixed both: the split converged
in ~17s instead of timing out at 90s, and the test still reliably covers
the (sub-second, ADR 0050 rung 8's F8 contract) freeze window with dense
enough probing. General form: a test that races a continuous write flood
against a background convergence loop must pace the flood below that
loop's own throughput, especially when the convergence condition itself
depends on catching up to the write volume — "hammer as fast as possible
until X happens" silently assumes X's own progress is independent of the
hammering, which is false whenever X is gated on draining exactly what's
being hammered in. (`crates/animusd/tests/split_build.rs::
probe_put_item_until_stopped`.)
