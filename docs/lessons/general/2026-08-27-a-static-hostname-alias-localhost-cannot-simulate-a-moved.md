# A static hostname alias (`localhost`) cannot simulate a moved endpoint — a real "same name, new address" test needs a name whose resolution the test itself can change (ADR 0060 advertise/dial split)

Testing "a node restarts on a different bind IP but keeps the same
advertised host, and peers still route to it" with `advertise_host:
Some("localhost")` seemed like the obvious choice — until the restarted
node rebound on `127.0.0.2` and the *other* node's dial of
`"localhost:{port}"` kept resolving to `127.0.0.1` (where nothing was
listening anymore), so the two-voter control group could never regain
quorum and the test hung at its second bootstrap wait until timeout.
`localhost` is a **static** `/etc/hosts` entry — nothing in the test moved
it, so of course it kept pointing at the address it always pointed at; the
real-world mechanism this ADR's advertise/dial split is built for (a
Kubernetes `Service`'s DNS updating when a pod is rescheduled) has no
sandboxed equivalent unless something in the test can actually make the
name re-resolve. Diagnosing this cost a debug-instrumented rerun (temporary
periodic `eprintln!` of both nodes' `is_control_leader`/`members`/
`node_addrs` inside the poll loop) that showed the restarted node's own
metadata forever empty — never even a rejected connection, just silence,
because the *leader's* traffic toward it was the direction silently going
nowhere. Fixed by managing a real, test-owned `/etc/hosts` entry (add
before bind, rewrite to the new IP at the moment of "restart", remove via
`Drop` so a panic mid-test still cleans up) — the only way in this sandbox
to make a hostname's resolution genuinely change mid-test, which is the
actual behavior being proven. General form: when a test needs to prove
"reachability follows a name, not an address, across the address
changing," a fixed alias proves nothing — either control the name's
resolution directly (as here) or design the assertion around what's
provable without dynamic re-resolution (e.g., the self-registered address
*string* staying unchanged, decoupled from whether a live connection can
currently complete).
