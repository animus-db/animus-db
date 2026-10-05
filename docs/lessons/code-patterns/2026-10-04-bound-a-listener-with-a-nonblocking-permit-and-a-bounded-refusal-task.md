# Bound a listener with a non-blocking permit, and give refusals their own bounded budget

A connection cap that merely `spawn`s a task which writes a 503 and closes has
only moved the unbounded fan-out: a flood of over-cap connections becomes a flood
of refusal tasks. The shape that works (R-01 (d), `animusd::overload`): the accept
loop calls `try_acquire` on an atomic `CountGate` (never an await), hands a
refused socket to a *second*, small gate of refusal tasks, and drops the socket
outright when that gate is also empty. Two more details that cost real time to
rediscover: a refusal must half-close and drain the peer's unread request bytes
for a short bounded time, otherwise closing with data in the receive buffer sends
an RST that can discard the 503 before the client reads it; and a TLS listener
cannot write plaintext HTTP before the handshake, so it closes outright.

Test it over real TCP, and prove "recovers when load drops" with a
converged-or-timeout poll; sheds under concurrency are non-vacuous only if the
test loops until the shed counter moves, not for a fixed duration.

Second lesson from the same task: before promising "a write error returns a named
error and recovers", read what the error path does today. Here every WAL
append/sync error is an `assert!` in the consensus loop, so the group dies and
`ProdEnv` keeps the process alive; recovery needs the persist round redesigned,
not an error mapping. See `docs/resource-bounds.md` section 3.
