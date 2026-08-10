# The control-plane write-ahead log (WAL)

How `animus-control` makes its Raft state durable: the per-node WAL, how it is
written, compacted, and recovered. See also
[ADR 0009](adr/0009-in-house-raft-over-env.md) and `crates/animus-control`
(`persist.rs`, `raft.rs`, `node.rs`).

**Since [ADR 0038](adr/0038-control-metadata-system-keyspace.md)** (`Metadata`
is `StateMachine::DRIVER_APPLIED`), this document's generic `RaftCore`
WAL/compaction mechanics below are still accurate as written, but two things
changed for the *control plane specifically*: (1) the `Snapshot{ metadata,
.. }` record's `metadata` field is now always the meaningless
`Metadata::default()` — the real durable state lives in a per-node
system-keyspace `StorageEngine`, not this record; (2) compaction (§3) is
driven from the async apply task's `engine_applied` watermark, not the
consensus loop's own `applied_since_snapshot()` — see `node.rs`'s
`meta_apply_and_compact`. The diagram below still describes exactly what
happens for an **in-core** (`DRIVER_APPLIED = false`) state machine, which is
what `generic_state_machine.rs`'s toy example (and no real state machine in
this codebase anymore) exercises.

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
└─ Snapshot  { metadata, last_index, last_term }  ← state-machine snapshot @ last_index
```

On disk: newline-delimited JSON, one record per line
(`encode_record` = `serde_json` + `\n`).

## 2. Write path — the core emits, the driver persists *before acting*

```
        RaftCore (pure, no I/O)                  driver (node.rs, owns Env)          Disk (Env seam)
        ───────────────────────                  ──────────────────────────          ───────────────
 handle()/tick()/propose()
   │ log_append(e)   → pending += Append(e)
   │ log_truncate(k) → pending += Truncate{k}    (suffix conflict fix)
   │ apply()         → updates the state machine (no record; snapshot covers it)
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

## 3. Snapshot & compaction — keep the file ≈ live tail

Once enough applied entries have piled up beyond the snapshot base, the driver
takes a snapshot (truncating the covered log prefix) and replaces the whole file
with a compact *image*.

```
 applied_since_snapshot() ≥ SNAPSHOT_THRESHOLD ?
                                   │ yes
                                   ▼
        core.snapshot()   ── advance snapshot base to last_applied,
                             drop log entries with index ≤ that base
                                   │
        core.wal_image()  =  [ Snapshot{meta, last_index, last_term}? ,  ← snapshot base (if any)
                               Hard{term,voted_for} ,                    ← current
                               Append(e) for e in log ]                  ← the *tail* after the base
                                   │  encode all → bytes
                                   ▼
        env.replace("raft.wal", bytes)  ──▶  ATOMIC swap of file contents
                                             (ProdEnv: tmp file + fsync + rename;
                                              SimEnv: durable = bytes under lock)
```

Same recovery result, bounded size — the prefix is gone, not just deduplicated:

```
BEFORE (every entry appended)                AFTER snapshot @ idx 81 + replace()
─────────────────────────                    ─────────────────────────
Hard{t=1,v=0}                                Snapshot{meta@81, last_index=81}
Append(idx1) … Append(idx81)                 Hard{t=1,v=0}
Append(idx82) Append(idx83) …                Append(idx82 ..)            ← only the tail
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
   term/vote/log-tail restored verbatim;  metadata + last_applied + commit_index
   set to the snapshot base (`last_index`);  role = Follower, election timer armed.
   The leader re-advances commit over the tail, re-applying it.
```

### Why re-apply the tail instead of restoring it?

Recovery restores the state machine to the **snapshot base** and re-applies the
log tail as commit re-advances. Because the base does *not* include those tail
entries, each committed command applies exactly once relative to it — so a
`CasTabletReplicas` lands once (epoch bumps once), never twice. (The earlier
per-apply-checkpoint scheme avoided re-apply entirely; this snapshot-base scheme
gets the same once-only guarantee while letting the log prefix be discarded.)

## Truncation & catch-up

The in-memory log is offset by the snapshot: it holds only entries with
`index > snapshot_index`. On a threshold (applied entries beyond the snapshot),
the node **snapshots** its applied state and drops the covered prefix
(`RaftCore::snapshot`), then rewrites the WAL to the now-smaller image — bounding
both the log and the WAL to the live tail.

A leader that has compacted past a follower's `next_index` can no longer build a
valid `AppendEntries` (the `prev` entry is gone), so it ships its snapshot whole
via an **`InstallSnapshot`** RPC; the follower replaces its state machine, sets
its snapshot base, and resumes from there.

## What this does *not* do (yet)

The snapshot ships in a single message (no chunked transfer), and a full
in-simulation process *restart-and-rejoin* is still pending — the simulator
can't yet stop and replace a node's tasks, so recovery is validated at the
`RaftCore` level (see `crates/animus-control/tests/wal_compaction.rs`,
`tests/persistence.rs`, and `tests/install_snapshot.rs`). Tracked in
[ADR 0009](adr/0009-in-house-raft-over-env.md).
