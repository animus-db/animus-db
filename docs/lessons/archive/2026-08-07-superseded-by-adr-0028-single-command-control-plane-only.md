# Superseded by ADR 0028 (single-command, control-plane-only tablet split)

ADR 0028 replaced the original two-phase tablet split (a control-plane
metadata write *plus* a separate data-plane `KvCommand::Split`/
`propose_split` command that could fail, race, or orphan independently) with
a single, epoch-CAS-gated `MetaCommand::SplitTablet` that is the *entire*
operation — the new sibling tablet's `StorageScope` covers already-present
data on the same node-shared storage engine (ADR 0026/0028), so there is no
handoff, no new-group bootstrap message, and no data-plane half left to fail
independently. Every mechanism below (the data-plane `Split` command,
`DropOrphanTablet`, the `pending` retry map, `current_split_bound`/
`SPLIT_BOUND_KEY`, the split hook + derived member ids, the `cp-hosted`
marker, `Coresident`/`sibling()` minting, `cp_member_id`/`cp_base_id`
translation, `propose_split_data`/`applied_split_key`, and
`claim_auto_split`/`release_auto_split`) no longer exists.
