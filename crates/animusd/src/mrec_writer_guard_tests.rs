//! ADR 0075 G-d M2: the structural guards that keep every base-row writer of
//! an MREC table on the stamped (evaluate-at-apply) path. The writer audit
//! itself is in ADR 0075's M2 amendment; these tests pin the two mechanisms it
//! leans on so a later change cannot silently reopen an unstamped writer.

use animus_control::schema::mrec_region_id;
use animus_control::{ApplyOutcome, ColumnType, MetaCommand, Metadata, TableSchema};

use crate::dynamo::table_change_records_carry_images;

fn world(mrec: bool) -> Metadata {
    let mut m = Metadata::default();
    assert_eq!(
        m.apply(&MetaCommand::CreateTableSchema {
            table: "t".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        }),
        ApplyOutcome::Applied
    );
    if mrec {
        assert_eq!(
            m.apply(&MetaCommand::ConvertTableToMrec {
                table: "t".into(),
                local_region: "eu".into(),
                region_id: mrec_region_id("eu"),
            }),
            ApplyOutcome::Applied
        );
    }
    m
}

/// The edge-valued fast arms (`fast_marker_write`, `marker_batch_write`) are
/// taken only when this is `false`; an MREC table must never take them (they
/// would write an unstamped base row), whatever else it declares.
#[test]
fn an_mrec_table_never_takes_the_edge_valued_fast_arm() {
    assert!(
        !table_change_records_carry_images(&world(false), "t"),
        "a plain table with no index/stream/PITR keeps the fast arm (byte-identical to before)"
    );
    assert!(
        table_change_records_carry_images(&world(true), "t"),
        "an MREC table must always take the evaluate-at-apply path"
    );
}
