# Don't react to "I was superseded" by *immediately* re-proposing higher

**Don't react to "I was superseded" by *immediately* re-proposing higher** —
that is the classic duelling-proposers **livelock** (two recoverers ratchet each
other's ballot forever within one logical instant, an unbounded message storm).
Break ties **deterministically** (e.g. only the higher-id contender retries; the
other stands down and adopts the winner's result) or back the retry off in time.
This also hangs a `SimEnv` test rather than failing it: the single-threaded
cooperative executor just spins at one virtual instant (100%+ CPU, no progress,
no panic), so **run new sim tests under a `timeout`** the first time — a hang
there is a same-instant unbounded-work loop, not slowness. (Found wiring Accord
recovery ballots; `animus-consensus` `core.rs::handle_superseded`.)
