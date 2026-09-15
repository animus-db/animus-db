# Writes with no change record are invisible to every change-log-derived copy/tail — inventory them before trusting O(delta)

**Writes with no change record are invisible to every change-log-derived
copy/tail — inventory them before trusting O(delta)** (ADR 0050 Train B
rung 5, 2026-08-17). Transaction decisions (`TxnCommit`/`TxnAbort`) and
resolves rewrite base rows without emitting any change record (ADR 0049
gave every *client* mutation a record; these apply-side rewrites predate
that contract). The split build's change-log tail therefore structurally
misses them: a child could inherit a stale `Pending` txn record for an
acked-committed transaction, and in-doubt recovery would later abort it —
silent acked-write loss. The v1 answer is a full final-image re-scan of
the frozen parent (state transfer, not log transfer — immune to signal
gaps by construction); the O(delta) restoration (apply-side markers for
signal-less rewrites) is a named follow-up. General form: before building
anything on "the change log sees every mutation," grep every apply arm
that calls the engine and list the ones that bypass record emission —
the tail is only as complete as that list is empty.
