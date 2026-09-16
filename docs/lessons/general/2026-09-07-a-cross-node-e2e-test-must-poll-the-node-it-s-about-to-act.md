# A cross-node e2e test must poll the node it's about to act through, not the node that made the earlier state change (ADR 0069, S-03 PR 2)

Writing `crates/animusd/tests/encryption_at_rest_segment_store_e2e.rs`'s
flagship test — `CreateBackup` against node 0, then `RestoreTableFromBackup`
against node 1 — the first draft polled `DescribeBackup` for `AVAILABLE`
against node 0 (the node that issued `CreateBackup`) before immediately
calling `RestoreTableFromBackup` against node 1. This flaked at roughly a
1-in-4 rate: node 1 legitimately observed `BackupInUseException` ("still
being created"), because the backup catalog is ordinary replicated
`Metadata` and node 1's own local apply of the `AVAILABLE` transition can
lag node 0's by a beat, independent of anything encryption-related. A
converged-or-timeout poll against the *wrong* node proves nothing about
what the node you're about to act through actually believes.

**General form**: when a test's next step targets node B based on a
condition it just confirmed on node A, the poll must run against B, not A
— "converged" is only meaningful for the specific reader that matters
next. This generalizes the root `CLAUDE.md`'s existing "eventual property
= converged-or-timeout poll, never a fixed-deadline one-shot assert" rule
one level further: it's not enough to poll *somewhere*, the poll has to be
against the party whose belief the next step actually depends on.
