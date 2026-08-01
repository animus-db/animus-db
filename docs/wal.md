# The control-plane write-ahead log (WAL)

How `custos-control` makes its Raft state durable: the per-node WAL, how it is
written, compacted, and recovered. See also
[ADR 0009](adr/0009-in-house-raft-over-env.md) and `crates/custos-control`
(`persist.rs`, `raft.rs`, `node.rs`).

The design keeps the consensus logic in a pure, I/O-free `RaftCore`: the core
*emits* records describing its durable-state changes, and the `Env`-driven
driver *writes* them. One append-only file per node (`raft.wal`) on the `Env`
disk.

## 1. Record types (`persist.rs::WalRecord`)

```
WalRecord
├─ Hard      { term, voted_for }              ← persisted term + vote (one current value)
├─ Append    (LogEntry{ term, index, cmd })   ← one per log entry appended
├─ Truncate  { keep }                         ← log shrank to `keep` entries (conflict fix)
└─ Snapshot  { metadata, last_applied }       ← state-machine checkpoint
```

On disk: newline-delimited JSON, one record per line
(`encode_record` = `serde_json` + `\n`).

## 2. Write path — the core emits, the driver persists *before acting*

```
        RaftCore (pure, no I/O)                  driver (node.rs, owns Env)          Disk (Env seam)
        ───────────────────────                  ──────────────────────────          ───────────────
 handle()/tick()/propose()
   │ log_append(e)   → pending += Append(e)
   │ log_truncate(k) → pending += Truncate{k}
   │ apply()         → pending += Snapshot{meta,last_applied}   (once per apply call)
   ▼
 drain_persist() ──────────────────────────────▶ for r in records:
   • checkpoint_hard():                             env.append("raft.wal", r) ─────────▶ buffered
       if (term,vote) changed → pending += Hard     env.sync("raft.wal")      ─────────▶ fsync ✓ durable
   • return mem::take(pending)                             │
                                                           ▼
                                                  THEN send outbound msgs (vote grant,
                                                  AppendEntries ack, …)
```

The ordering is the safety rule: a granted vote / acknowledged append is
**durable before it leaves the node**. The driver flushes at the top of its loop
(to catch a client `propose`) and again after each `handle`/`tick` (before
sending the responses that depend on it).

## 3. Compaction — keep the file ≈ live state

The driver counts records appended since the last rewrite; past a threshold it
replaces the whole file with a compact *image*.

```
 flush_wal() returns N  ──▶  since_compaction += N
                                   │
                        since_compaction ≥ WAL_COMPACT_THRESHOLD ?
                                   │ yes
                                   ▼
        core.wal_image()  =  [ Snapshot{meta,last_applied}? ,   ← latest only (omitted if nothing applied)
                               Hard{term,voted_for} ,           ← current
                               Append(e) for e in log ]         ← whole current log
                                   │  encode all → bytes
                                   ▼
        env.replace("raft.wal", bytes)  ──▶  ATOMIC swap of file contents
                                             (ProdEnv: tmp file + fsync + rename;
                                              SimEnv: durable = bytes under lock)
        since_compaction = 0
```

Same recovery result, bounded size:

```
BEFORE (grows per apply)                     AFTER replace()
─────────────────────────                    ─────────────────────────
Hard{t=1,v=0}                                Snapshot{meta@81, la=81}   ← 1 checkpoint
Append(idx1 noop)                            Hard{t=1,v=0}
Append(idx2) Snapshot{la=2}                  Append(idx1 .. idx81)      ← current log
Append(idx3) Snapshot{la=3}
… 80 more Append + 80 more Snapshot …        (per-apply Snapshot churn gone)
```

`replace` is atomic, so a crash mid-rewrite leaves the **whole old** or **whole
new** WAL — never a torn file. A torn *append* (a partial trailing line from a
crash mid-write) is also tolerated: `decode` drops an unparsable last line.

## 4. Recovery — replay the fold on startup (`drive` → `RaftCore::recovered`)

```
 env.read("raft.wal")  →  bytes
        │ PersistedState::decode   (split on '\n', parse each line, ignore torn tail)
        ▼
   records: Vec<WalRecord>
        │ PersistedState::replay  — fold in order:
        │     Hard      → term, voted_for                   (last wins)
        │     Append(e) → log.push(e)
        │     Truncate  → log.truncate(keep)
        │     Snapshot  → snapshot = (metadata, last_applied)   (last wins)
        ▼
   PersistedState { term, voted_for, log, snapshot }
        │ RaftCore::recovered(id, peers, state, now, entropy)
        ▼
   term/vote/log restored verbatim;  metadata + last_applied + commit_index
   restored from the checkpoint  →  committed commands are NOT re-applied
   (so a CAS isn't double-applied);  role = Follower, election timer armed.
```

### Why a checkpoint instead of replaying the log into the state machine?

Re-applying committed entries on recovery would double-apply non-idempotent
commands — e.g. `CasTabletReplicas` bumps an epoch each time. The `Snapshot`
record carries the already-applied `metadata` + `last_applied`, so recovery
jumps straight there and only the *uncommitted* tail of the log is left for the
leader to drive.

## What this does *not* do (yet)

The in-memory log still keeps **all** entries — the WAL is bounded to the *live
state*, not to a constant. Truncating the committed log prefix (true Raft log
compaction) additionally needs an `InstallSnapshot` RPC to catch up a follower
that has fallen behind the compaction point; that is noted as remaining work in
[ADR 0009](adr/0009-in-house-raft-over-env.md). Full in-simulation
restart-and-rejoin is likewise pending (recovery is currently validated at the
`RaftCore` level — see `crates/custos-control/tests/wal_compaction.rs` and
`tests/persistence.rs`).
