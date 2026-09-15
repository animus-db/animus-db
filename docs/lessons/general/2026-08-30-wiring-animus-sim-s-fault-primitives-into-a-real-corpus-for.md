# Wiring `animus-sim`'s fault primitives into a real corpus for the first time (ADR 0061 Decision 3, `raftkv_linearizable.rs`)

`animus-sim`'s deepened fault vocabulary (`NetConfig::set_duplicate_prob`/
`set_corrupt_prob`/`set_heavy_tail_prob`, `DiskConfig::set_fsync_lie_prob`/
`torn_tail_on_crash`/`corrupt_on_crash`) had been proven correct in its own
meta-tests (`animus-sim/tests/net_faults.rs`, `disk_faults.rs`) but never
actually driven through a real protocol-correctness corpus. Wiring it into
the flagship raftkv linearizability corpus surfaced two lessons, one a
harness trap and the other a real, still-open production finding.

**1. A `Nemesis` that sets a global `Env` fault config for "the rest of the
run" must be reset by the runner's heal step, or it silently keeps firing
past its own scenario's intended fault window.** Every pre-existing
`Nemesis` variant in this corpus only ever touched partitions/crashes
(healed by `Simulator::heal`/`restart`) or `NetConfig` (reset by
`heal_all`'s pre-existing `self.sim.set_net_config(NetConfig::default())`).
The moment a variant (`FsyncLie`) set a global `DiskConfig` for the first
time, `heal_all` needed the identical reset
(`self.sim.set_disk_config(DiskConfig::default())`) or the lie would keep
firing for every scenario cell that happens to run after it in the same
process — invisible in this corpus's own harness (each scenario gets a
fresh `Simulator`, so nothing here actually leaked), but exactly the trap a
future corpus sharing one `Simulator` across cells (or a future `Nemesis`
composing several fault-window phases within one scenario) would fall
into silently. General rule: every new global-fault-setting `Nemesis`
needs its own explicit line in whatever "return to steady state" step the
harness already has for the fault classes that came before it — don't
assume the existing reset call already covers a fault dimension it
predates.

**2. A wire codec that "never panics" can still abort the whole process —
and this corpus caught a real instance of it within minutes of running at
depth.** The task briefing (based on a prior investigation) asserted
`NetConfig::set_corrupt_prob` was "verified safe" for this corpus because
`animus_cp_data::codec::decode_wire` is bounds-checked and its
`KvWire`/`RaftMsg` match sites `warn!`/drop on a decode `Err` rather than
panicking. That's true for the primitive per-byte reads (`Cursor::u8`/`u32`/
`u64`/`bytes` all bounds-check via `take()` before ever touching memory) but
false for a whole class of *array* decoders: `read_raft`'s `AppendEntries`
arm (`codec.rs`) reads an untrusted `n: u32` entry count straight off the
wire and calls `Vec::with_capacity(n as usize)` **before** a single one of
those `n` entries has been validated against the cursor's remaining bytes.
A single bit-flip landing inside those 4 length bytes can turn `n` into
something close to `u32::MAX`; since each `LogEntry<KvCommand>` is a
multi-field struct, `with_capacity` then requests on the order of a
terabyte, which Rust's allocator failure path (`handle_alloc_error`)
**aborts the process on** — not a catchable panic, and nothing a
`std::panic::catch_unwind` or a `Result`-based decode error contract can
intercept. Reproduced deterministically at `ANIMUS_RAFTKV_SEEDS=20`:
`chaos_early_3_s09`, seed `422907917505132688`, `SIGABRT`. Grepping
`codec.rs` for the same `let n = c.u32()?; ... Vec::with_capacity(n as
usize)` shape found **at least a dozen more sites** (`read_kind_writes`,
`read_change_logs`, `TxnStage`'s `puts`/`conditions`, `SplitTablet`'s
`replicas`, `TxnRecord`'s `spans`/`conditions`, `SeedBatch`'s `rows`, and
`read_raft`'s own `AppendEntries`/`InstallSnapshot`-adjacent arms) — this is
a **systemic gap in the codec's array-decoding pattern**, not a one-off in
one match arm. Deliberately **not fixed as part of this corpus PR** (the
repo's own "an incidental bug gets its own PR" rule, root `CLAUDE.md`
Conventions) — `Nemesis::Chaos` ships without `set_corrupt_prob` instead
(see its own doc in `raftkv_linearizable.rs`), the same "primitive excluded
from this corpus, real fix deferred to its own crate/PR" treatment this
corpus already gives `DiskConfig::set_enospc_prob`/`set_error_prob` for the
identical hard-abort-vs-graceful-`Err` reason. **The general lesson**: when
a decode path's own doc (or a fault-injection plan) claims "bounds-checked,
never panics," verify that claim against every *array*-shaped decoder
specifically — a scalar bounds check (`take(n)` before slicing) is not the
same guarantee as a *count*-driven pre-allocation being bounded, and the
difference is invisible until something (a corrupted wire byte, a
malformed peer, a fuzzer) actually supplies an adversarial count. The real
fix (tracked as a follow-up, not implemented here) is straightforward:
either drop the `with_capacity` hint for array decoders entirely (a
`push`-per-iteration loop already bounds itself correctly via `Cursor::
take`'s own check, just without the allocation being *pre*-sized) or cap
the hint at some cheap function of the cursor's actual remaining byte
length before ever calling `with_capacity`.
