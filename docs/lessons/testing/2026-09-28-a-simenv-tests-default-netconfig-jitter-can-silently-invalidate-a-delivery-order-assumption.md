# A `SimEnv` test's default `NetConfig` jitter can silently invalidate a "delivery order == send order" assumption

**What happened.** Writing the per-stream inbox-cap fault corpus (ADR
0026's 2026-09-28 inbox-cap amendment), a test asserted that draining a
capped, drop-oldest stream after sending `N` frames (well past the cap)
would yield exactly the newest `CAP` frames, **in send order** — the
natural way to word "drop-oldest retains the newest". It failed
immediately, and not with an off-by-one: the retained frames were a
plausible-looking but wrong subset, e.g. `[..., 17, 18]` where `[..., 18,
19]` was expected. The cause had nothing to do with the cap logic itself:
`NetConfig::default()` draws up to 4ms of uniform jitter per message
(`max_jitter: Duration::from_millis(4)`), and each of the `N` frames' `send
_stream` calls draws its **own, independent** jitter — so with all `N`
frames sent at the same virtual instant (spawned in a tight loop before
`run_for` ever advances the clock), their `deliver_at` times differ by up
to 4ms in an order that has nothing to do with send order. The timeline's
`(time, seq)` ordering then delivers them in that shuffled order, not send
order — the cap's own drop-oldest logic was completely correct; the test's
assumption about which frame arrived "oldest" was wrong.

**The general shape.** Any `SimEnv` test that sends several messages in a
tight loop and then asserts something about the **order frames arrive** at
a receiver (not just totals/counts) is implicitly relying on `deliver_at`
being monotonic in send order. That holds under a `NetConfig` with zero
jitter (a fixed `base_delay` alone, ties broken by the monotonically
increasing `seq`), but **not** under the crate's own default `NetConfig`,
which exists specifically to model real jitter and is what every test gets
unless it explicitly overrides it. This is easy to miss because:

- A test that only checks aggregate behavior (a value eventually arrives,
  a count converges, a set of keys is present) never notices — jitter only
  matters once "which one arrived first" is part of the assertion.
- The failure looks like a logic bug in whatever mechanism is under test
  (here: "the cap must be evicting the wrong frame"), not a test-harness
  ordering assumption, so it's tempting to debug the production code first.

**What to do.** Before asserting on delivery **order** in a `SimEnv` test
(not just delivery outcome), either:

- Set `max_jitter: Duration::ZERO` on the `NetConfig` used for that
  scenario (directly, or via a small helper — see `crates/animus-sim/
  tests/inbox_cap.rs`'s `deterministic_order_net_config`), so `deliver_at`
  differs only by `seq`, which tracks send order for messages sent at the
  same virtual instant; or
- Serialize the sends against an observable side effect (poll until frame
  `i` is accounted for — queued, applied, or otherwise visible — before
  sending frame `i + 1`), which sidesteps jitter entirely by construction
  (see `crates/animus-env/src/prod.rs`'s `ProdEnv`-side sibling test, which
  has the identical problem for an unrelated reason: `send_stream` itself
  schedules its real write onto a background task, so even *that* crate's
  send-call order doesn't guarantee wire order without serializing).

Either fix is a few lines; skipping this and chasing the "wrong" production
code instead costs far more. The same caution applies to any future
`SimEnv` test asserting FIFO/priority/eviction order over multiple
messages, not just this cap.
