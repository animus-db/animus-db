# A SimEnv relay timeout cannot tell "confirmed dead" from "merely slow" the way a real TCP stack can — a hinted forward hop must be capped either way

Found characterizing issue #900's own boot-path entropy-desync collateral
in `sim_cluster_data_only.rs`'s `c_crash_of_a_data_only_replica_holder_
the_rest_keep_serving_then_it_catches_up_over_seeds` (seed `3665440779`): a
survivor's write, issued immediately after a data-only replica crashed,
stalled outright instead of merely running slow — confirmed by re-running
the exact seed with `CLIENT_TIMEOUT`/`OP_BUDGET` both temporarily
multiplied 10x, which still failed identically (thousands more internal
relay attempts, never a single reply).

## The trap: a design that is safe under ProdEnv can be silently unsafe under SimEnv, for a reason that has nothing to do with SimEnv's own determinism guarantee

`ClientCtx::forward_to_tablet_leader`'s hinted-retry chase (issue #316,
then #585, then #585's own two continuations) is a carefully tuned piece
of production logic: a *guessed* forward candidate (no vouching signal)
gets its own hop capped at `FORWARD_HOP_TIMEOUT` (2s), so a chase can
always fall back to another tablet replica within `CLIENT_TIMEOUT` (10s)
even if its first guess is wrong. A *hinted* candidate (a live replica's
own leader hint) was deliberately left **uncapped** — it gets the entire
remaining budget — because issue #585's own regression showed a
genuinely slow-but-live leader during a membership-change storm can
legitimately need several seconds, well past `FORWARD_HOP_TIMEOUT`, to
answer, and capping it would wrongly abandon a leader that was about to
succeed.

That reasoning has an unstated precondition: **a hinted-but-actually-dead
candidate must fail fast**, so the "uncapped" hop only ever really *waits*
for the genuinely-alive-but-slow case. Under `ProdEnv`, this holds:
`relay_request_with_timeout`'s `TcpStream::connect` against a crashed
peer's socket fails (or is refused) at the OS/network layer well under a
second, producing `RELAY_TRANSPORT_FAILURE` — a fast, confirmed-dead
signal, long before any timeout could fire. Under `SimEnv`,
`SimRelayClient::relay` has **no equivalent signal at all**: it sends a
message and polls for a reply until its own `timeout` elapses — a
crashed/partitioned peer and a merely slow one are byte-for-byte
indistinguishable from the caller's point of view, since in both cases
*nothing arrives* until the timeout fires. The "uncapped hinted hop is
safe because a dead one fails fast" precondition is simply false under
`SimEnv`, so an uncapped hinted hop to a peer that crashed the instant
before the hint was read consumes the **entire** budget waiting for a
reply that will never come — leaving zero time left for the very
hinted-retry chase mechanism (issue #316/#585) that exists to handle
exactly this case.

A second, independent, compounding gap: `SimRelayClient`'s own timeout
error text never matched either sentinel string
(`RELAY_HOP_TIMEOUT`/`RELAY_TRANSPORT_FAILURE`) the chase's classification
checks for — those are `relay_request_with_timeout`'s own, `ProdEnv`-only
shapes. So even a *shorter* hinted hop's failure under `SimEnv` would have
fallen through to the chase's terminal "genuine application failure" arm
instead of its "try another known replica" arm.

## The fix

1. Export a stable prefix constant for `SimRelayClient`'s own timeout
   error (`animus_node::sim_relay::SIM_RELAY_TIMEOUT_PREFIX`) and
   recognize it in `forward_to_tablet_leader`'s classification as
   equivalent to `RELAY_HOP_TIMEOUT` (never `RELAY_TRANSPORT_FAILURE` —
   `SimEnv` genuinely cannot tell "confirmed dead" from "merely slow", so
   the conservative classification, which keeps the candidate eligible for
   an immediate retry rather than permanently excluding it, is the only
   honest one).
2. Cap a hinted hop at a new, more generous-but-bounded constant,
   `HINTED_FORWARD_HOP_TIMEOUT` (6s of the 10s `CLIENT_TIMEOUT`) instead
   of the whole remaining budget — sized to comfortably preserve issue
   #585's own "several seconds, well past `FORWARD_HOP_TIMEOUT`" scenario
   while guaranteeing the chase always has some budget left (4s) to try
   another replica if the hint really was stale.

Both changes apply identically under `ProdEnv` and `SimEnv` — deliberately
not a `SimEnv`-only branch. Under `ProdEnv` a dead hinted peer already
resolves via `RELAY_TRANSPORT_FAILURE` well inside 6s, so the cap is
essentially never the binding constraint there; it is the *only* thing
that ever bounds a dead hinted peer's own hop under `SimEnv`.

## The generalizable rule

When a retry/timeout budget allocation is tuned around a implicit
liveness assumption ("X fails fast, so it's safe to wait a long time for
X"), that assumption is a property of the **transport**, not of the
retry logic itself — and a test harness's own transport substitute
(a `SimEnv`-native relay, a fake client, an in-memory stub) does not
automatically inherit it. Before trusting a production retry/backoff
design to behave the same way under a simulated or faked transport,
check specifically: does the substitute transport produce the *same
failure-mode-and-latency shape* the design's own tuning assumed, for
every distinct failure case the real transport can produce (here:
"connection refused instantly" vs. "connected but no reply for the full
timeout" are two different real-world shapes with two different
required timeout bounds, and a substitute that only ever produces the
second is a hidden narrowing of the transport's own failure space, not
merely determinism). This is a distinct, sharper form of the "SimEnv
proves logic and ordering, not real-thread liveness" house lesson: here
`SimEnv` was fully deterministic and the ordering was exactly as
intended — the bug was that the *simulated transport's own failure model*
was blunter (and therefore differently timed) than the real one the
production code was tuned against, not that anything was nondeterministic
or out of order.

## How this was found, for the general method

A test seed that "stopped converging" is not proof of a real stall by
itself — but it is not proof of a benign timing artifact either. The
only way to tell them apart is to **multiply the relevant timeout/budget
constants by 10x (a temporary, uncommitted local edit) and re-run the
exact failing seed**: if it now passes, the seed was merely slow and a
re-pin (with the concrete mechanism/timing documented) is legitimate; if
it still fails identically — same error, same request id, thousands more
internal attempts and no more progress — that is conclusive proof of a
genuine stall, not a timing coincidence, and it must be root-caused, not
re-pinned away. This is exactly the discipline the sibling entry on issue
#811 (a corpus seed that shifted straight into a real apply-loop
livelock after issue #667's own entropy-shifting fix) already
establishes for the control plane; this entry is the CP-forwarding-layer
instance of the same principle.
