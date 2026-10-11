//! ADR 0078 section 3: `/admin/health` carries an advisory `hosting_ramp`
//! boolean plus (hosted, deferred, unsettled) counts, read from the gauge the
//! node's host reconciler publishes. Advisory means it can never change the
//! verdict: a ramping node is `200`/`ok` exactly as a settled one.
//!
//! In-crate `#[cfg(test)] mod`, sibling to `sim_cluster_throttle`, for the
//! same reason: it needs `SimCluster`'s `pub(crate)` surface.

use animus_cp_data::host::HostingRamp;

use super::sim_cluster::SimCluster;

fn health(cluster: &mut SimCluster, node: u64) -> (u16, serde_json::Value) {
    let (status, body) = cluster.admin(node, "GET", "/admin/health", "", &[]);
    let json = serde_json::from_str(&body).unwrap_or_else(|e| panic!("bad JSON {e}: {body}"));
    (status, json)
}

#[test]
fn health_reports_a_settled_node_as_not_ramping_with_its_hosted_count() {
    let mut cluster = SimCluster::new(0x4841_0078, 3, 3);
    cluster.create_table("orders");

    let (status, body) = health(&mut cluster, 0);
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ok"], true, "{body}");
    assert_eq!(body["hosting_ramp"], false, "{body}");
    assert_eq!(body["hosting_ramp_deferred"], 0, "{body}");
    assert!(
        body["hosting_ramp_hosted"].as_u64().is_some_and(|n| n >= 1),
        "the node hosts the table's tablet: {body}"
    );
}

#[test]
fn a_hosting_ramp_is_advisory_and_never_fails_health() {
    let mut cluster = SimCluster::new(0x4841_0079, 3, 3);
    cluster.create_table("orders");

    // Seed a mid-ramp reading on node 0 (the reconciler would publish this
    // while a large restart hosts in waves).
    cluster
        .handle()
        .node_ctx(0)
        .edge
        .hosting_ramp()
        .store(HostingRamp {
            hosted: 100,
            deferred: 200,
            unsettled: 37,
        });

    let (status, body) = health(&mut cluster, 0);
    assert_eq!(status, 200, "a ramp must not fail the health check: {body}");
    assert_eq!(body["ok"], true, "{body}");
    assert_eq!(body["hosting_ramp"], true, "{body}");
    assert_eq!(body["hosting_ramp_hosted"], 100, "{body}");
    assert_eq!(body["hosting_ramp_deferred"], 200, "{body}");
    assert_eq!(body["hosting_ramp_unsettled"], 37, "{body}");

    // Readiness and the roll gate are unaffected too.
    let (rs, rbody) = cluster.admin(0, "GET", "/admin/ready", "", &[]);
    assert_eq!(rs, 200, "{rbody}");
    let (hs, hbody) = cluster.admin(0, "GET", "/admin/roll-health", "", &[]);
    assert!(
        !hbody.contains("hosting_ramp"),
        "roll-health must not consult the advisory ramp ({hs}): {hbody}"
    );

    // Once the reconciler's next tick publishes a settled reading, it clears.
    cluster
        .handle()
        .node_ctx(0)
        .edge
        .hosting_ramp()
        .store(HostingRamp::default());
    let (_, body) = health(&mut cluster, 0);
    assert_eq!(body["hosting_ramp"], false, "{body}");
}
