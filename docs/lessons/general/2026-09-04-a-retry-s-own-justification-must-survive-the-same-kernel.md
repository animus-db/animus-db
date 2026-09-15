# A retry's own justification must survive the same kernel-semantics scrutiny as the bug it claims to fix (issue #592, closed without a code fix)

Issue #592 reported `crates/animusd/tests/index_backfill.rs` panicking
`connect: Connection refused` against a `bring_up`-issued address on PR
#586's CI run (job 101112281267, "prod-liveness-animusd (1/4)" —
confirmed by pulling the real job log via `mcp__github__get_job_logs`,
not just the issue's own paraphrase). A first pass at a fix added
`support::connect_retry` — a bounded 5s retry on `ErrorKind::
ConnectionRefused`, modeled on `restart_same_addrs`'s existing rebind
retry — reasoning the failure was the same "port-TOCTOU/listener-not-
yet-accepting" family that helper already documents.

**That reasoning does not survive scrutiny, and the fix was reverted.**
`TcpListener::bind` (what `Node::bind`/every `Bound*Node::start_*` calls,
`crates/animusd/src/lib.rs:4483-4610`) performs a synchronous OS-level
`bind()`+`listen()` before the `.await` ever resolves — by the time
`run_node`'s `?` lets a listener field be assigned, the kernel's own SYN
backlog is already authoritative and completes a handshake **without
userspace scheduling being involved at all**. Once `listen()` has
returned, a later `connect()` to that same live socket cannot legitimately
observe `ECONNREFUSED` from scheduler delay, however severe — that error
means nothing was bound at that address at that instant, not "accept
hasn't run yet" (a full backlog drops SYNs, which surfaces as a
**timeout**, never a refusal). Since every one of `bring_up`'s nodes
already returned `Ok` — meaning every one of its five listeners
individually completed `bind()+listen()` — before the test's first
`call()` ever dials one, and since every dial here is same-process
loopback (all "nodes" in this fixture are `Node` structs inside the one
test process, not separate OS processes) with no way for another process
to silently evict an already-open listening fd, no scheduler-timing story
closes the gap.

**Investigation, in order:**
1. Reread every step between `free_addrs`'s probe-release and the test's
   own `client = config.nodes[leader].intra` dial — `bring_up`'s
   per-attempt retry (`crates/animusd/tests/index_backfill.rs`, the
   `'attempts` loop) discards a **whole** attempt's nodes/config on any
   node's `run_node` failure and starts the next attempt with entirely
   fresh addresses, so a stale/mismatched address from a failed earlier
   attempt cannot leak into the returned `(nodes, config)` — confirmed by
   reading the loop, not assumed.
2. Instrumented a scratch copy of `connect_retry` (dumping `ss -ltnp` on
   first refusal) and `bring_up`/the test body (printing every node's
   configured vs. actually-bound address, and live leader status) — never
   observed a mismatch across every run that did complete, including the
   one run (immediately after a cold, disk-heavy compile) that reproduced
   a 5-second sustained refusal locally.
3. That one local repro turned out to have an unrelated, confirmed cause:
   this session's sandbox was, at the time, sharing one `$CARGO_TARGET_DIR`
   across multiple concurrently-building agent worktrees — Cargo omits a
   path dependency's location from its artifact hash, so their builds were
   overwriting each other's rlibs. A binary linked from a partially
   clobbered rlib is exactly the kind of "the process is running but not
   behaving as its own source implies" state that would explain a
   synchronously-bound listener never actually working — once builds moved
   to a private `CARGO_TARGET_DIR`, the exact same test could not be
   reproduced again in over 35 runs, including several under deliberately
   added CPU load.
4. Pulled the real failing CI job's log (`mcp__github__get_job_logs`, not
   just `gh api`'s redirect to blob storage, which this sandbox's proxy
   blocks) rather than trusting the issue's paraphrase. Two findings that
   change the shape of the theory: **the job runs under `cargo-nextest`**
   (a separate OS process per test — `index_backfill.rs`'s three tests
   never ran concurrently with each other in that CI run, ruling out the
   in-file sibling-test port race the original doc comment speculated
   about), and the failing test's own total runtime was **3.34s with no
   retry logic at all yet** — a fast, clean `bring_up`, then one single
   refused connect on the very first attempt, not a sustained multi-second
   outage. No `Too many open files`/OOM/disk-space signal appears anywhere
   in that job's log; the only other failure in the same shard was the
   already separately root-caused `split_placing_two_replica_diff_e2e`
   (issue #585's own lineage).

**Outcome**: `connect_retry` was removed rather than kept — a retry whose
own justification cannot survive "what would have to be true for this
error to occur" is exactly the "wider timeout papering over an
unroot-caused flake" the green invariant (`CLAUDE.md` Session operating
mode item 4) forbids, even though it happened to make the local repro go
away (for an unrelated reason — see point 3). The three call sites reverted
to a bare `TcpStream::connect(addr).await`, with one durable improvement
kept: the panic message now names the address (`"connect to {addr} failed:
{e}"`) instead of a bare `.expect("connect")`, so a **future** occurrence
is immediately actionable without needing a special debugging session to
even know which of a multi-node cluster's addresses failed. Issue #592
stays open with this write-up rather than being closed as fixed — the true
mechanism behind the original one-shot CI refusal remains unconfirmed;
the strongest lead is that CI's own build/cache pipeline could suffer an
analogous corruption to point 3 above, but that is not established, only
plausible.

**General form**: when a retry (or any other "paper over the symptom"
fix) is proposed for a fault, ask what would have to be true at the OS/
runtime level for that fault to occur — a socket/lock/file-handle API's
real semantics often rule out entire classes of "just scheduling delay"
explanations outright, and a fix whose only support is "it looks like the
timing-window family we've seen before" needs that support checked before
being trusted, not after it ships. A single local reproduction is also
not enough corroboration for a specific mechanism on its own if the
reproduction environment has an independent, unrelated defect (here: a
shared build cache across concurrent agents) capable of producing the
identical symptom for a completely different reason — isolate the
environment first, then judge whether the original theory still
reproduces.
