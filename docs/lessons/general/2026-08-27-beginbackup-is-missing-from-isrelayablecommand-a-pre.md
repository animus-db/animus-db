# `BeginBackup` is missing from `is_relayable_command` — a pre-existing Train 1 gap, found but out of scope (ADR 0059 §9, Train 3)

While auditing the relay allowlist for the new PITR `MetaCommand`s
(`UpdateContinuousBackups`/`SealPitrSegment`/`MarkBackupPitrBase`, all
added to `is_relayable_command`), a grep for every existing `MetaCommand::
BeginBackup` construction site turned up that `MetaCommand::BeginBackup`
itself is **not** on the allowlist, despite `dynamo.rs::create_backup`
calling `ctx.propose_schema(&MetaCommand::BeginBackup { .. })` — the exact
same "may run on any node, must relay to the control leader" shape every
other DDL-class command (`SetTableTtl`, `SetTableStream`, etc.) already has
an allowlist entry for. `ClientCtx::propose_schema` relays via
`ClientRequest::ProposeSchema` whenever the local node has no live control
leader handle of its own; the receiving node's `is_relayable_command` gate
would reject `BeginBackup` outright, and `create_backup`'s own commit-wait
loop would then exhaust `CREATE_BACKUP_ID_ATTEMPTS` retries (minting a
fresh id each time, since it can't distinguish "rejected" from "never
reached a leader") and finally return a timeout error — meaning
`CreateBackup` issued against a **follower-connected** node in a real
multi-node deployment likely fails today, every time, until a data-role
node happens to become the control leader.

Not fixed here: this is a genuine, real bug, but it predates this PR
(Train 1 PR④), is unrelated to PITR's own mechanism, and root `CLAUDE.md`'s
own engineering practices are explicit that "an incidental pre-existing bug
discovered during a task gets its own separate PR ... never a drive-by fix
folded into an unrelated diff." Recorded here so it isn't silently
rediscovered later: the fix is one line (`MetaCommand::BeginBackup { .. }`
added to `is_relayable_command`'s allowlist) plus a
`schema_ddl_relay.rs`-style regression test mirroring `update_time_to_live_
on_a_follower_is_relayed_to_the_leader`. **Generalizable lesson**: when a
new wire operation's `MetaCommand` gets a relay-allowlist entry, grep for
every *sibling* command proposed the same way (same wire-handler shape,
same `ctx.propose_schema` call) while you're in the allowlist anyway — a
gap next to the one you're fixing is exactly the kind of thing a narrowly-
scoped PR walks right past.
