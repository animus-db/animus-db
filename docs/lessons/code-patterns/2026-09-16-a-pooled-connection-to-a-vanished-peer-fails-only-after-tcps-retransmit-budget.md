# A pooled connection to a peer that vanished at its old IP fails only after TCP's retransmit budget unless the socket carries a user timeout — "reconnect on error" never fires when the write itself never errors

**A pooled connection to a peer that vanished at its old IP fails only
after TCP's retransmit budget unless the socket carries a user timeout —
"reconnect on error" never fires when the write itself never errors.**
`animus-env`'s `ProdEnv` pools one outbound TCP connection per peer
address string and reconnects **once** whenever a write on it returns an
error (issue #661) — a correct, sufficient fix for a peer that *closes*
its connection (a restart, a graceful shutdown: the kernel always sends a
FIN or RST as part of any socket teardown, whatever the app's own exit
path). It is not sufficient for a peer that **vanishes without ever
closing anything** — the exact shape of a Kubernetes pod recreated at a
new IP under its old stable DNS name, when the old pod's network
namespace is torn down before (or races) its final FIN going out. From
the sender's kernel's point of view nothing is wrong: `write()` still
succeeds instantly, because the bytes only need to reach the *local*
kernel send buffer, not the peer — TCP retransmits the unacknowledged data
silently in the background, and only reports a failure once its own
retry budget (`tcp_retries2`, commonly 13–15 exponential-backoff retries,
13–15 **minutes** on Linux by default) is exhausted. A "reconnect on
write error" path checks a condition that, for this specific failure
mode, simply never becomes true within any timeframe a caller cares
about — the bug is not that the reconnect logic is wrong, it's that the
signal it waits for doesn't exist yet.

The fix is to give the *socket itself* a bounded failure signal, not to
add more logic above it: TCP keepalive (`TCP_KEEPIDLE`/`TCP_KEEPINTVL`/
`TCP_KEEPCNT`) plus, where the OS supports it (Linux/Android/Fuchsia/
Cygwin), `TCP_USER_TIMEOUT` — the latter is the one that actually matters
for an actively-writing connection, since it bounds "how long transmitted
data may remain unacknowledged" directly, independent of whether the
connection also goes idle. Once that bound elapses, the *next* write
genuinely does return an error, and the pre-existing "reconnect once on
error" logic (which was already correct) finally has something to react
to. `tokio::net::TcpStream` exposes no setter for either of these — reach
through `socket2::SockRef` (a borrow, not an ownership transfer) on the
already-connected/-accepted socket, both on the dial side and the accept
side, so detection is symmetric regardless of which end of the connection
a given node happens to be on, and independent of whether TLS is layered
on top (both options operate below the TLS record layer). Keepalive alone
(the necessary fallback on platforms without `TCP_USER_TIMEOUT`) is a
strictly weaker guarantee worth naming explicitly: a keepalive probe is
deliberately constructed to fall within the peer's already-acknowledged
window, so a live-but-unresponsive-application peer — its kernel fully
functional, just not reading — **still answers it**, meaning keepalive
alone cannot tell "the peer is gone" apart from "the peer's app is merely
slow to read." Only a platform with an unacked-data timeout closes that
gap.

**Testing this is harder than it looks, and the difficulty is itself
informative.** On a single host/kernel, closing a socket (even via
`SIGKILL` on the peer process) always causes the kernel to emit a FIN or
RST as part of fd cleanup — there is no userspace-reachable way to close
a connection "silently." An accepted connection that simply never reads
also doesn't reproduce the bug: TCP still acknowledges everything it
already buffered, and answers keepalive probes regardless, so it only
ever produces an ordinary flow-control stall, not the "no ack, ever"
condition this fix targets — a real, narrower, and already-understood
scenario, not this one. The only hermetic, single-host construction that
actually reproduces "no acknowledgment ever, from an already-established
connection" is dropping the sender's own outbound packets to the old
peer's address at the network layer (an `iptables OUTPUT -d <old-ip> -j
DROP` rule) while re-pointing the peer's *stable* registered address
string (a hostname, via a controlled `/etc/hosts` entry — matching
production's actual DNS-based indirection) at a second, live listener on
the same port. Changing the registered address string itself is not a
valid substitute: if the pooled-connection cache is keyed by that string
(the correct design, since resolution is meant to happen fresh on every
dial), changing the string is a guaranteed cache miss that "fixes itself"
via a plain fresh dial regardless of whether the underlying keepalive/
user-timeout fix exists at all — a test built that way would pass before
and after the fix, proving nothing.
