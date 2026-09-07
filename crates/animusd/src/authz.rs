//! ADR 0066 §5/§6 — per-key allow-list enforcement at dispatch: after a
//! SigV4-authenticated request resolves to a [`Principal`]
//! (`dynamo.rs::handle_conn`, wrapping `animus_node::sigv4_gate::
//! AuthOutcome`), every operation's resolved [`OpClass`] and named table(s)
//! are checked against the principal's own [`Policy`] before the operation
//! runs. See that ADR's Decision 5 for the full design this module
//! implements; this file is deliberately small — one classification match,
//! one predicate, one error builder.

use animus_control::{OpClass, Policy, TableMatch};
use animus_dynamo::wire::{Operation, WireError};
use animus_env::Metric;

use crate::ClientCtx;

/// The authenticated caller's authorization scope, resolved once per
/// connection at the SigV4 gate (`dynamo.rs::handle_conn`) and threaded
/// down to [`authorize`]/[`authorize_each_table`]/[`authorize_unscoped`] at
/// dispatch. Carries no secret — only what a denial's error message names.
#[derive(Clone, Debug)]
pub(crate) enum Principal {
    /// SigV4 is disabled entirely (no `--dynamo-auth` and an empty
    /// replicated credential catalog), or the caller matched the static
    /// bootstrap credential (ADR 0066 §4) — unrestricted, the pre-S-02
    /// behaviour.
    Unrestricted,
    /// The caller matched a replicated catalog row (ADR 0066 §1) — scoped
    /// to that row's own [`Policy`].
    Scoped {
        access_key_id: String,
        region: String,
        policy: Policy,
    },
}

impl Principal {
    /// The [`Unrestricted`](Self::Unrestricted) principal — every call site
    /// that runs a request with no SigV4 gate in front of it at all (the
    /// admin dashboard's `POST /admin/data/dynamo` proxy, ADR 0021; the
    /// animusd console; the internal admin seeder) uses this, exactly
    /// matching pre-S-02 behaviour for every surface but the real DynamoDB
    /// wire edge.
    pub(crate) fn unrestricted() -> Self {
        Principal::Unrestricted
    }
}

impl From<animus_node::sigv4_gate::AuthOutcome> for Principal {
    fn from(outcome: animus_node::sigv4_gate::AuthOutcome) -> Self {
        match outcome.policy {
            Some(policy) => Principal::Scoped {
                access_key_id: outcome.access_key_id,
                region: outcome.region,
                policy,
            },
            // `CredentialSource::Bootstrap` — the static bootstrap
            // credential is unrestricted (ADR 0066 §4).
            None => Principal::Unrestricted,
        }
    }
}

/// ADR 0066 §1's operation→class mapping (Decision 1's table), made
/// concrete: `(wire operation name, its OpClass)` for every [`Operation`]
/// variant. **Exhaustive — no wildcard arm** — a new `Operation` variant is
/// a compile error here until it is deliberately classified, the same
/// discipline `animus_node::wire::is_relayable_command`/`surface_of`
/// already apply to their own enums (root `CLAUDE.md`'s "grep every gating
/// match site" lesson). The three table-less operations (`ListTables`/
/// `DescribeLimits`/`DescribeEndpoints`) still get a class here for
/// totality, but [`authorize_op`] never consults it for them — ADR 0066
/// Decision 1's "needs no class or table check at all" exception is
/// enforced by never calling `Policy::allows` for them, not by this
/// function returning something meaningless.
pub(crate) fn classify(op: &Operation) -> (&'static str, OpClass) {
    match op {
        Operation::GetItem { .. } => ("GetItem", OpClass::Read),
        Operation::BatchGetItem { .. } => ("BatchGetItem", OpClass::Read),
        Operation::Query { .. } => ("Query", OpClass::Read),
        Operation::Scan { .. } => ("Scan", OpClass::Read),
        Operation::TransactGetItems { .. } => ("TransactGetItems", OpClass::Read),
        // ADR 0071 (W-07 PR 2/3): `Operation::ExecuteStatement` carries its
        // PartiQL `statement` as opaque, unparsed text — the real statement
        // kind (`SELECT` vs. `INSERT`/`UPDATE`/`DELETE`) is a fact about
        // *content*, not about which `Operation` variant this is, and this
        // function has no catalog/parser access to look inside the string.
        // So this row stays `OpClass::Read` unconditionally, always has
        // (never revisited to distinguish PR 3's mutation shapes) — it is
        // NOT the real enforcement point for a mutation. `authorize_op`
        // (below) is a deliberate no-op for `ExecuteStatement` for the
        // identical reason; the actual per-statement-kind authorization
        // happens inside `crate::dynamo::execute_statement` itself, once
        // `statement` is parsed: a `SELECT` gets an explicit `authz::
        // authorize(.., OpClass::Read, ..)` call there, and an `INSERT`/
        // `UPDATE`/`DELETE` lowers onto a *real* `Operation::PutItem`/
        // `UpdateItem`/`DeleteItem` and runs through `run_operation`, whose
        // own `authorize_op` call classifies **that** concrete operation —
        // `OpClass::Write`, per this table's own `PutItem`/`UpdateItem`/
        // `DeleteItem` rows below — before any write executes. See
        // `every_operation_classifies_per_adr_0066_decision_1`'s own
        // `ExecuteStatement` cases (covering all four statement kinds) for
        // why they all assert the identical `OpClass::Read` here despite
        // three of them being mutations, and this PR's own ADR 0071
        // amendment for the full account.
        Operation::ExecuteStatement { .. } => ("ExecuteStatement", OpClass::Read),
        // `BatchExecuteStatement` (ADR 0071, W-07 PR 4) carries a whole
        // *array* of opaque, unparsed `statement` texts — exactly
        // `ExecuteStatement`'s own "no catalog/parser access here" reason,
        // just with more than one of them. This row stays `OpClass::Read`
        // unconditionally for the identical reason (never revisited to
        // distinguish a batch of mutations from a batch of reads) — it is
        // NOT the real enforcement point. `authorize_op` (below) is a
        // deliberate no-op for it too; real per-statement authorization
        // happens inside `crate::dynamo::execute_one_batch_statement`, once
        // each statement is individually parsed and its own table known —
        // a `SELECT` gets an explicit `authz::authorize` call there; an
        // `INSERT`/`UPDATE`/`DELETE` reuses `execute_statement`, whose own
        // `INSERT`/`UPDATE`/`DELETE` branches already authorize through
        // `run_operation`'s `authorize_op` call on the real, now-concrete
        // `PutItem`/`UpdateItem`/`DeleteItem` operation.
        Operation::BatchExecuteStatement { .. } => ("BatchExecuteStatement", OpClass::Read),
        // `ExecuteTransaction` (ADR 0071, W-07 PR 5) is the identical shape
        // one level up: `statements` is a list of opaque, unparsed PartiQL
        // texts, so this function can't tell an all-`SELECT` transaction
        // from an all-mutation one any more than `ExecuteStatement` can
        // tell `SELECT` from `INSERT`/`UPDATE`/`DELETE`. Stays
        // `OpClass::Read` unconditionally for the same reason — the real
        // enforcement happens inside `crate::dynamo::execute_transaction`
        // once every statement is parsed: an all-`SELECT` transaction calls
        // `run_transact_get` (whose own `authz::authorize_each_table` uses
        // `OpClass::Read`), and a write transaction calls `run_transact`
        // (whose own `authorize_each_table` uses `OpClass::Write`) — both
        // check every named table, whole-set, before anything runs.
        // `authorize_op` (below) is a deliberate no-op for
        // `ExecuteTransaction` for the identical reason.
        Operation::ExecuteTransaction { .. } => ("ExecuteTransaction", OpClass::Read),
        Operation::DescribeTable { .. } => ("DescribeTable", OpClass::Read),
        Operation::DescribeTimeToLive { .. } => ("DescribeTimeToLive", OpClass::Read),
        Operation::ListTagsOfResource { .. } => ("ListTagsOfResource", OpClass::Read),
        Operation::ListTables { .. } => ("ListTables", OpClass::Read),
        Operation::DescribeLimits => ("DescribeLimits", OpClass::Read),
        Operation::DescribeEndpoints => ("DescribeEndpoints", OpClass::Read),

        Operation::PutItem { .. } => ("PutItem", OpClass::Write),
        Operation::UpdateItem { .. } => ("UpdateItem", OpClass::Write),
        Operation::DeleteItem { .. } => ("DeleteItem", OpClass::Write),
        Operation::BatchWriteItem { .. } => ("BatchWriteItem", OpClass::Write),
        Operation::TransactWriteItems { .. } => ("TransactWriteItems", OpClass::Write),

        Operation::CreateTable { .. } => ("CreateTable", OpClass::Ddl),
        Operation::UpdateTable { .. } => ("UpdateTable", OpClass::Ddl),
        Operation::DeleteTable { .. } => ("DeleteTable", OpClass::Ddl),
        Operation::UpdateTimeToLive { .. } => ("UpdateTimeToLive", OpClass::Ddl),
        Operation::TagResource { .. } => ("TagResource", OpClass::Ddl),
        Operation::UntagResource { .. } => ("UntagResource", OpClass::Ddl),
        Operation::UpdateContinuousBackups { .. } => ("UpdateContinuousBackups", OpClass::Ddl),

        Operation::CreateBackup { .. } => ("CreateBackup", OpClass::Backup),
        Operation::DescribeBackup { .. } => ("DescribeBackup", OpClass::Backup),
        Operation::ListBackups { .. } => ("ListBackups", OpClass::Backup),
        Operation::DeleteBackup { .. } => ("DeleteBackup", OpClass::Backup),
        Operation::RestoreTableFromBackup { .. } => ("RestoreTableFromBackup", OpClass::Backup),
        Operation::RestoreTableToPointInTime { .. } => {
            ("RestoreTableToPointInTime", OpClass::Backup)
        }
        Operation::DescribeContinuousBackups { .. } => {
            ("DescribeContinuousBackups", OpClass::Backup)
        }

        // S3 export (ADR 0068, S-05) — same `OpClass::Backup` class as the
        // rest of the backup/restore family (ADR 0066 Decision 1's own
        // table groups every backup-shaped operation together).
        Operation::ExportTableToPointInTime { .. } => ("ExportTableToPointInTime", OpClass::Backup),
        Operation::DescribeExport { .. } => ("DescribeExport", OpClass::Backup),
        Operation::ListExports { .. } => ("ListExports", OpClass::Backup),

        // S3 import (ADR 0068 §6, S-05 PR 2) — same `OpClass::Backup` class
        // as the export/restore family just above.
        Operation::ImportTable { .. } => ("ImportTable", OpClass::Backup),
        Operation::DescribeImport { .. } => ("DescribeImport", OpClass::Backup),
        Operation::ListImports { .. } => ("ListImports", OpClass::Backup),
    }
}

/// Authorize a single-table (or table-less/backup-id-keyed) operation
/// before [`crate::dynamo::run_operation`] dispatches it. **Exhaustive over
/// every [`Operation`] variant, no wildcard** — mirrors [`classify`]'s own
/// discipline, so a new variant is a compile error here too until someone
/// decides how it resolves a table (or deliberately joins the table-less/
/// multi-table-per-item-inside-its-own-handler groups below).
///
/// `BatchGetItem`/`BatchWriteItem`/`TransactGetItems`/`TransactWriteItems`
/// are multi-table and checked **inside their own handlers**, per table,
/// before any of that request's work runs (ADR 0066 §5) — see
/// [`authorize_each_table`]; this function is a deliberate no-op for all
/// four. `ListTables`/`DescribeLimits`/`DescribeEndpoints` need no class or
/// table check at all (ADR 0066 Decision 1). `DescribeBackup`/
/// `DeleteBackup` resolve the checked table from the backup's own recorded
/// source table (`BackupRow::table`, ADR 0059 §3) — `None` (the backup ARN
/// doesn't resolve to a row at all) lets the request continue to its own
/// `BackupNotFoundException`, never granting or denying based on a fact
/// that isn't there. `ListBackups` with no `TableName` filter is an
/// unscoped, cluster-wide read — allowed only to a credential whose own
/// policy is `TableMatch::All` (see [`authorize_unscoped`]'s doc).
pub(crate) fn authorize_op(
    ctx: &ClientCtx,
    principal: &Principal,
    op: &Operation,
    meta: &animus_control::Metadata,
) -> Result<(), WireError> {
    let (name, class) = classify(op);
    match op {
        Operation::BatchGetItem { .. }
        | Operation::BatchWriteItem { .. }
        | Operation::TransactWriteItems { .. }
        | Operation::TransactGetItems { .. } => Ok(()),

        // `ExecuteStatement`'s table is only known once its `statement` is
        // parsed (ADR 0071) — this function runs before that parse, so it
        // is a deliberate no-op here too, joining the group above.
        // `crate::dynamo::execute_statement` authorizes the real target
        // table itself, before any read runs, once parsing has resolved it.
        Operation::ExecuteStatement { .. } => Ok(()),

        // `BatchExecuteStatement`'s own statements' tables are only known
        // once each is individually parsed (ADR 0071, W-07 PR 4) — a
        // deliberate no-op here too, joining `ExecuteStatement`'s group for
        // the identical reason. `crate::dynamo::execute_one_batch_statement`
        // authorizes each statement's real target table itself, once
        // parsing has resolved it, and turns a denial into that one
        // statement's own `AccessDenied` response entry rather than
        // rejecting the whole batch — unlike `BatchGetItem`/`BatchWriteItem`
        // above, which reject the whole request (ADR 0071's own "no
        // cross-statement atomicity" contract extends to authorization too:
        // one denied statement must not block its siblings).
        Operation::BatchExecuteStatement { .. } => Ok(()),

        // `ExecuteTransaction`'s tables are only known once every one of
        // its statements is parsed (ADR 0071, W-07 PR 5) — this function
        // runs before that parse, so it is a deliberate no-op here too,
        // joining `ExecuteStatement`'s group. `crate::dynamo::
        // execute_transaction` authorizes the real target tables itself
        // (whole-set, via `authz::authorize_each_table` inside
        // `run_transact`/`run_transact_get`), before any read/write runs,
        // once parsing has resolved them.
        Operation::ExecuteTransaction { .. } => Ok(()),

        Operation::ListTables { .. } | Operation::DescribeLimits | Operation::DescribeEndpoints => {
            Ok(())
        }

        Operation::DescribeBackup { backup_arn } | Operation::DeleteBackup { backup_arn } => {
            let table = meta.backup(backup_arn).map(|row| row.table.as_str());
            authorize(ctx, principal, name, class, table)
        }
        Operation::ListBackups { table, .. } => match table {
            Some(t) => authorize(ctx, principal, name, class, Some(t.as_str())),
            None => authorize_unscoped(ctx, principal, name, class),
        },

        // S3 export (ADR 0068, S-05) — same shape as the backup family
        // just above: `DescribeExport` resolves the checked table from the
        // export's own recorded source table (`ExportRow::table`), `None`
        // when the ARN doesn't resolve to a row (lets the request continue
        // to its own `ExportNotFoundException`); `ListExports` with no
        // `TableArn` filter is unscoped, exactly like `ListBackups`.
        Operation::DescribeExport { export_arn } => {
            let table = meta.export(export_arn).map(|row| row.table.as_str());
            authorize(ctx, principal, name, class, table)
        }
        Operation::ListExports { table_arn, .. } => match table_arn {
            Some(arn) => authorize(
                ctx,
                principal,
                name,
                class,
                animus_dynamo::wire::parse_table_arn(arn),
            ),
            None => authorize_unscoped(ctx, principal, name, class),
        },

        Operation::CreateTable { table, .. }
        | Operation::UpdateTable { table, .. }
        | Operation::DescribeTable { table, .. }
        | Operation::DeleteTable { table, .. }
        | Operation::PutItem { table, .. }
        | Operation::GetItem { table, .. }
        | Operation::DeleteItem { table, .. }
        | Operation::Query { table, .. }
        | Operation::Scan { table, .. }
        | Operation::UpdateItem { table, .. }
        | Operation::UpdateTimeToLive { table, .. }
        | Operation::DescribeTimeToLive { table, .. }
        | Operation::UpdateContinuousBackups { table, .. }
        | Operation::DescribeContinuousBackups { table, .. }
        | Operation::CreateBackup { table, .. }
        | Operation::ExportTableToPointInTime { table, .. }
        | Operation::TagResource { table, .. }
        | Operation::UntagResource { table, .. }
        | Operation::ListTagsOfResource { table, .. } => {
            authorize(ctx, principal, name, class, Some(table.as_str()))
        }
        Operation::RestoreTableFromBackup {
            target_table_name, ..
        }
        | Operation::RestoreTableToPointInTime {
            target_table_name, ..
        } => authorize(
            ctx,
            principal,
            name,
            class,
            Some(target_table_name.as_str()),
        ),

        // S3 import (ADR 0068 §6, S-05 PR 2) — `ImportTable` targets its
        // brand-new table, named inside `TableCreationParameters` (the
        // identical "the table about to exist" reasoning
        // `RestoreTableFromBackup` above already has); `DescribeImport`/
        // `ListImports` mirror `DescribeExport`/`ListExports` exactly.
        Operation::ImportTable {
            table_creation_params,
            ..
        } => authorize(
            ctx,
            principal,
            name,
            class,
            Some(table_creation_params.table_name.as_str()),
        ),
        Operation::DescribeImport { import_arn } => {
            let table = meta.import(import_arn).map(|row| row.target_table.as_str());
            authorize(ctx, principal, name, class, table)
        }
        Operation::ListImports { table_arn, .. } => match table_arn {
            Some(arn) => authorize(
                ctx,
                principal,
                name,
                class,
                animus_dynamo::wire::parse_table_arn(arn),
            ),
            None => authorize_unscoped(ctx, principal, name, class),
        },
    }
}

/// Authorize one `class` against one `table` for `principal` — the single
/// predicate every call site in this module (and `dynamo.rs`'s
/// `BatchGetItem`/`BatchWriteItem`/`run_transact`/`run_transact_get`
/// per-table pre-checks) shares. `table: None` is a deliberate no-op (used
/// only by [`authorize_op`]'s backup-id-keyed arms when the id doesn't
/// resolve to a row yet) — never a security decision on its own.
pub(crate) fn authorize(
    ctx: &ClientCtx,
    principal: &Principal,
    op_name: &str,
    class: OpClass,
    table: Option<&str>,
) -> Result<(), WireError> {
    let Principal::Scoped {
        access_key_id,
        region,
        policy,
    } = principal
    else {
        return Ok(());
    };
    let Some(table) = table else {
        return Ok(());
    };
    if policy.allows(class, Some(table)) {
        Ok(())
    } else {
        record_denied(ctx);
        Err(access_denied_error(op_name, table, access_key_id, region))
    }
}

/// [`authorize`]'s multi-table sibling — used by `BatchGetItem`/
/// `BatchWriteItem`/`TransactGetItems`/`TransactWriteItems`'s own handlers
/// (`dynamo.rs`) to check **every** table a request names before any of its
/// work runs, so a request spanning an allowed and a denied table is
/// rejected whole rather than partially applied (ADR 0066 §5).
pub(crate) fn authorize_each_table<'a>(
    ctx: &ClientCtx,
    principal: &Principal,
    op_name: &str,
    class: OpClass,
    tables: impl IntoIterator<Item = &'a str>,
) -> Result<(), WireError> {
    for table in tables {
        authorize(ctx, principal, op_name, class, Some(table))?;
    }
    Ok(())
}

/// Authorize a **cross-table, unfiltered** operation (today, only
/// `ListBackups` with no `TableName`) — allowed to an
/// [`Unrestricted`](Principal::Unrestricted) (bootstrap) principal, and to
/// a [`Scoped`](Principal::Scoped) one only
/// when its own policy is scoped to [`TableMatch::All`] **and** allows
/// `class`; a table-restricted policy can never safely answer "every
/// backup of every table I might not even be allowed to see," so it is
/// denied outright rather than silently narrowed.
pub(crate) fn authorize_unscoped(
    ctx: &ClientCtx,
    principal: &Principal,
    op_name: &str,
    class: OpClass,
) -> Result<(), WireError> {
    let Principal::Scoped {
        access_key_id,
        region,
        policy,
    } = principal
    else {
        return Ok(());
    };
    if policy.ops.contains(&class) && matches!(policy.tables, TableMatch::All) {
        Ok(())
    } else {
        record_denied(ctx);
        Err(access_denied_error(op_name, "*", access_key_id, region))
    }
}

/// `Metric::AuthDenied` (ADR 0066 §5/§9) — every DynamoDB request reaching
/// this module ran through the SigV4 gate first, which only ever builds a
/// `ClientCtx` reachable from the bound `dynamo` listener (combined/
/// data-only nodes, ADR 0035 PR3) — the same structural guarantee
/// `ClientCtx::describe_endpoints`'s own `ctx.data()` call relies on.
fn record_denied(ctx: &ClientCtx) {
    ctx.data().raftkv_metrics.incr(Metric::AuthDenied);
}

/// AWS-shaped `AccessDeniedException` message (ADR 0066 §5): names the
/// operation and the (synthesized) resource ARN, never the policy's own
/// contents. `access_key_id`/`region` come straight from the caller's own
/// `Credential` scope (ADR 0057's "never pinned" region convention) — this
/// codebase has no account-id/IAM-user concept beyond the access key id
/// itself, hence the fixed placeholder account `000000000000`.
fn access_denied_error(op_name: &str, table: &str, access_key_id: &str, region: &str) -> WireError {
    WireError::access_denied(format!(
        "User: arn:aws:iam::000000000000:user/{access_key_id} is not authorized to perform: \
         dynamodb:{op_name} on resource: arn:aws:dynamodb:{region}:000000000000:table/{table}"
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use animus_control::OpClass;
    use animus_dynamo::capacity::{ReturnConsumedCapacity, ReturnItemCollectionMetrics};
    use animus_dynamo::wire::{
        BackupTypeFilter, Operation, ReturnValues, ReturnValuesOnConditionCheckFailure, Select,
        TransactStatementRequest, UpdateReturnValues,
    };
    use animus_item::{Item, TableSchema};

    use super::{Policy, classify};

    /// Every [`Operation`] variant, minimally constructed, paired with the
    /// wire name/class ADR 0066 §1's mapping table names for it — pinning
    /// [`classify`]'s exhaustive match against a drift. A future edit that
    /// reclassifies a variant (or a compiler error from an unclassified new
    /// one) shows up as a diff here, mirroring `wire::tests::
    /// classification_is_pinned`'s own precedent for
    /// `is_relayable_command`.
    #[test]
    fn every_operation_classifies_per_adr_0066_decision_1() {
        let table = || "t".to_string();
        let item = || Item::new();
        let cases: Vec<(Operation, &str, OpClass)> = vec![
            (
                Operation::CreateTable {
                    table: table(),
                    schema: TableSchema::simple("pk"),
                    key_types: vec![],
                    indexes: vec![],
                    stream_view_type: None,
                    throughput: None,
                },
                "CreateTable",
                OpClass::Ddl,
            ),
            (
                Operation::UpdateTable {
                    table: table(),
                    stream: None,
                    index_update: None,
                    key_types: vec![],
                    throughput_update: None,
                },
                "UpdateTable",
                OpClass::Ddl,
            ),
            (
                Operation::DescribeTable { table: table() },
                "DescribeTable",
                OpClass::Read,
            ),
            (
                Operation::DeleteTable { table: table() },
                "DeleteTable",
                OpClass::Ddl,
            ),
            (
                Operation::ListTables {
                    exclusive_start_table_name: None,
                    limit: None,
                },
                "ListTables",
                OpClass::Read,
            ),
            (
                Operation::PutItem {
                    table: table(),
                    item: item(),
                    condition: None,
                    return_values: ReturnValues::None,
                    capacity: ReturnConsumedCapacity::default(),
                    metrics: ReturnItemCollectionMetrics::default(),
                },
                "PutItem",
                OpClass::Write,
            ),
            (
                Operation::GetItem {
                    table: table(),
                    key: item(),
                    projection: None,
                    consistent_read: false,
                    capacity: ReturnConsumedCapacity::default(),
                },
                "GetItem",
                OpClass::Read,
            ),
            (
                Operation::DeleteItem {
                    table: table(),
                    key: item(),
                    condition: None,
                    return_values: ReturnValues::None,
                    capacity: ReturnConsumedCapacity::default(),
                    metrics: ReturnItemCollectionMetrics::default(),
                },
                "DeleteItem",
                OpClass::Write,
            ),
            (
                Operation::Query {
                    table: table(),
                    index: None,
                    partition_attr: "pk".to_string(),
                    partition_value: animus_item::AttributeValue::S("v".to_string()),
                    sort_attr: None,
                    sort_condition: None,
                    limit: None,
                    exclusive_start_key: None,
                    scan_index_forward: true,
                    filter: None,
                    projection: None,
                    select: Select::default(),
                    consistent_read: false,
                },
                "Query",
                OpClass::Read,
            ),
            (
                Operation::Scan {
                    table: table(),
                    index: None,
                    limit: None,
                    exclusive_start_key: None,
                    filter: None,
                    projection: None,
                    select: Select::default(),
                    segment: None,
                    consistent_read: false,
                },
                "Scan",
                OpClass::Read,
            ),
            (
                Operation::UpdateItem {
                    table: table(),
                    key: item(),
                    actions: vec![],
                    condition: None,
                    return_values: UpdateReturnValues::default(),
                    capacity: ReturnConsumedCapacity::default(),
                    metrics: ReturnItemCollectionMetrics::default(),
                },
                "UpdateItem",
                OpClass::Write,
            ),
            (
                Operation::BatchWriteItem {
                    requests: BTreeMap::new(),
                },
                "BatchWriteItem",
                OpClass::Write,
            ),
            (
                Operation::TransactWriteItems {
                    actions: vec![],
                    token: None,
                },
                "TransactWriteItems",
                OpClass::Write,
            ),
            (
                Operation::BatchGetItem { requests: vec![] },
                "BatchGetItem",
                OpClass::Read,
            ),
            (
                Operation::TransactGetItems { gets: vec![] },
                "TransactGetItems",
                OpClass::Read,
            ),
            (
                Operation::UpdateTimeToLive {
                    table: table(),
                    attribute_name: "ttl".to_string(),
                    enabled: true,
                },
                "UpdateTimeToLive",
                OpClass::Ddl,
            ),
            (
                Operation::DescribeTimeToLive { table: table() },
                "DescribeTimeToLive",
                OpClass::Read,
            ),
            (
                Operation::CreateBackup {
                    table: table(),
                    backup_name: "b".to_string(),
                },
                "CreateBackup",
                OpClass::Backup,
            ),
            (
                Operation::DescribeBackup {
                    backup_arn: "arn".to_string(),
                },
                "DescribeBackup",
                OpClass::Backup,
            ),
            (
                Operation::ListBackups {
                    table: None,
                    limit: None,
                    exclusive_start_backup_arn: None,
                    time_range_lower_bound_ms: None,
                    time_range_upper_bound_ms: None,
                    backup_type: BackupTypeFilter::default(),
                },
                "ListBackups",
                OpClass::Backup,
            ),
            (
                Operation::DeleteBackup {
                    backup_arn: "arn".to_string(),
                },
                "DeleteBackup",
                OpClass::Backup,
            ),
            (
                Operation::RestoreTableFromBackup {
                    backup_arn: "arn".to_string(),
                    target_table_name: table(),
                    global_secondary_index_override: None,
                },
                "RestoreTableFromBackup",
                OpClass::Backup,
            ),
            (
                Operation::RestoreTableToPointInTime {
                    source_table_name: table(),
                    target_table_name: table(),
                    restore_date_time_ms: None,
                    use_latest_restorable_time: true,
                    global_secondary_index_override: None,
                },
                "RestoreTableToPointInTime",
                OpClass::Backup,
            ),
            (
                Operation::ExportTableToPointInTime {
                    table: table(),
                    table_arn: "arn".to_string(),
                    s3_bucket: "bucket".to_string(),
                    s3_prefix: None,
                    export_time_ms: None,
                    client_token: None,
                },
                "ExportTableToPointInTime",
                OpClass::Backup,
            ),
            (
                Operation::DescribeExport {
                    export_arn: "arn".to_string(),
                },
                "DescribeExport",
                OpClass::Backup,
            ),
            (
                Operation::ListExports {
                    table_arn: None,
                    max_results: None,
                    next_token: None,
                },
                "ListExports",
                OpClass::Backup,
            ),
            (
                Operation::ImportTable {
                    s3_bucket: "bucket".to_string(),
                    s3_prefix: None,
                    input_compression: animus_control::InputCompressionType::None,
                    table_creation_params: animus_dynamo::wire::TableCreationParameters {
                        table_name: table(),
                        schema: TableSchema::simple("pk"),
                        key_types: vec![],
                        indexes: vec![],
                        throughput: None,
                    },
                    client_token: None,
                },
                "ImportTable",
                OpClass::Backup,
            ),
            (
                Operation::DescribeImport {
                    import_arn: "arn".to_string(),
                },
                "DescribeImport",
                OpClass::Backup,
            ),
            (
                Operation::ListImports {
                    table_arn: None,
                    page_size: None,
                    next_token: None,
                },
                "ListImports",
                OpClass::Backup,
            ),
            (
                Operation::UpdateContinuousBackups {
                    table: table(),
                    enabled: true,
                },
                "UpdateContinuousBackups",
                OpClass::Ddl,
            ),
            (
                Operation::DescribeContinuousBackups { table: table() },
                "DescribeContinuousBackups",
                OpClass::Backup,
            ),
            (
                Operation::TagResource {
                    table: table(),
                    tags: BTreeMap::new(),
                },
                "TagResource",
                OpClass::Ddl,
            ),
            (
                Operation::UntagResource {
                    table: table(),
                    tag_keys: vec![],
                },
                "UntagResource",
                OpClass::Ddl,
            ),
            (
                Operation::ListTagsOfResource { table: table() },
                "ListTagsOfResource",
                OpClass::Read,
            ),
            // ADR 0071 (W-07 PR 2/3): `classify`'s `ExecuteStatement` row
            // cannot see inside `statement` (it's opaque, unparsed text at
            // this layer) — every one of the four statement kinds
            // classifies identically here, `OpClass::Read`, **including**
            // the three mutation shapes. That is not a gap: the real
            // per-statement-kind enforcement happens once `dynamo::
            // execute_statement` has parsed `statement` and lowered a
            // mutation onto a genuine `PutItem`/`UpdateItem`/`DeleteItem`
            // `Operation` (the `OpClass::Write` rows already covered above)
            // — see `classify`'s own doc comment on this row for the full
            // account.
            (
                Operation::ExecuteStatement {
                    statement: "SELECT * FROM t".to_string(),
                    parameters: vec![],
                    consistent_read: false,
                    next_token: None,
                    limit: None,
                    return_consumed_capacity: ReturnConsumedCapacity::None,
                },
                "ExecuteStatement",
                OpClass::Read,
            ),
            (
                Operation::ExecuteStatement {
                    statement: "INSERT INTO t VALUE {'pk':?}".to_string(),
                    parameters: vec![],
                    consistent_read: false,
                    next_token: None,
                    limit: None,
                    return_consumed_capacity: ReturnConsumedCapacity::None,
                },
                "ExecuteStatement",
                OpClass::Read,
            ),
            (
                Operation::ExecuteStatement {
                    statement: "UPDATE t SET a = ? WHERE pk = ?".to_string(),
                    parameters: vec![],
                    consistent_read: false,
                    next_token: None,
                    limit: None,
                    return_consumed_capacity: ReturnConsumedCapacity::None,
                },
                "ExecuteStatement",
                OpClass::Read,
            ),
            (
                Operation::ExecuteStatement {
                    statement: "DELETE FROM t WHERE pk = ?".to_string(),
                    parameters: vec![],
                    consistent_read: false,
                    next_token: None,
                    limit: None,
                    return_consumed_capacity: ReturnConsumedCapacity::None,
                },
                "ExecuteStatement",
                OpClass::Read,
            ),
            // ADR 0071 (W-07 PR 4): `BatchExecuteStatement` classifies
            // `OpClass::Read` unconditionally too, for the identical "opaque
            // statement text, no catalog here" reason `ExecuteStatement`'s
            // own cases above document — see `classify`'s own doc comment
            // on this row.
            (
                Operation::BatchExecuteStatement {
                    statements: vec![],
                    return_consumed_capacity: ReturnConsumedCapacity::None,
                },
                "BatchExecuteStatement",
                OpClass::Read,
            ),
            // ADR 0071 (W-07 PR 5): `ExecuteTransaction`'s own `classify`
            // row has the identical "can't see inside opaque statement
            // text" shape as `ExecuteStatement`'s row just above — an
            // all-`SELECT` transaction and an all-mutation one both
            // classify `OpClass::Read` here, real enforcement deferred to
            // `dynamo::execute_transaction` once every statement is parsed.
            (
                Operation::ExecuteTransaction {
                    statements: vec![TransactStatementRequest {
                        statement: "SELECT * FROM t WHERE pk = ?".to_string(),
                        parameters: vec![],
                        rvocf: ReturnValuesOnConditionCheckFailure::None,
                    }],
                    token: None,
                    return_consumed_capacity: ReturnConsumedCapacity::None,
                },
                "ExecuteTransaction",
                OpClass::Read,
            ),
            (
                Operation::ExecuteTransaction {
                    statements: vec![TransactStatementRequest {
                        statement: "INSERT INTO t VALUE {'pk':?}".to_string(),
                        parameters: vec![],
                        rvocf: ReturnValuesOnConditionCheckFailure::None,
                    }],
                    token: None,
                    return_consumed_capacity: ReturnConsumedCapacity::None,
                },
                "ExecuteTransaction",
                OpClass::Read,
            ),
            (Operation::DescribeLimits, "DescribeLimits", OpClass::Read),
            (
                Operation::DescribeEndpoints,
                "DescribeEndpoints",
                OpClass::Read,
            ),
        ];
        for (op, expected_name, expected_class) in &cases {
            let (name, class) = classify(op);
            assert_eq!(name, *expected_name, "op name for {op:?}");
            assert_eq!(class, *expected_class, "class for {expected_name}");
        }
    }

    #[test]
    fn policy_allow_all_permits_every_class_but_admin() {
        let p = Policy::allow_all();
        for class in [
            OpClass::Read,
            OpClass::Write,
            OpClass::Ddl,
            OpClass::Streams,
            OpClass::Backup,
        ] {
            assert!(p.allows(class, Some("any-table")));
        }
        assert!(!p.ops.contains(&OpClass::Admin));
    }
}
