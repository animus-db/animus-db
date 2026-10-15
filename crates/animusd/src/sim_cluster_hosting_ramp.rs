//! ADR 0078 section 3: `/admin/health` carries an advisory `hosting_ramp`
//! boolean plus (hosted, deferred, unsettled) counts, read from the gauge the
//! node's host reconciler publishes. Advisory means it can never change the
//! verdict: a ramping node is `200`/`ok` exactly as a settled one.
//!
//! In-crate `#[cfg(test)] mod`, sibling to `sim_cluster_throttle`, for the
//! same reason: it needs `SimCluster`'s `pub(crate)` surface.

use std::time::Duration;

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

/// Issue #1315: a node restarted a moment ago answers the admin API at once,
/// is still `Active` in `Metadata` (its death is not yet detected) and, until
/// its reconciler's first pass, hosts none of its tablets. `roll-health` used
/// to say `ok` then (no hosted group means no group fails clause 6), so
/// `animus cluster roll wait` returned in milliseconds and the roll killed
/// the next node while the first was serving nothing. It must report the
/// un-hosted tablets, and converge to `ok` once the node has re-hosted them.
#[test]
fn roll_health_is_not_ok_for_a_restarted_node_until_it_re_hosts_its_tablets() {
    let mut windows = 0u32;
    for seed in 0..4u64 {
        // `LsmEngine` backend: a `Memory` restart is a wiped disk, not a
        // process restart (the node would come back without `Metadata`).
        let mut cluster = SimCluster::new_with_lsm_engines(
            0x4841_1315 + seed,
            &[crate::config::NodeRole::Both; 3],
            3,
            None,
        );
        let tablet = cluster.create_table("orders");
        cluster.run_for(Duration::from_secs(2));
        assert_eq!(
            cluster.roll_health_view(1)["ok"],
            true,
            "seed {seed}: a settled node is ok: {}",
            cluster.roll_health_view(1)
        );

        cluster.restart(1);
        // Step in 5 ms slices. At every instant: while the node's `Metadata`
        // names it a replica of the tablet but it has not hosted the group,
        // the verdict must not be `ok`. (`windows` counts the slices that
        // state was observed in, so the test cannot pass vacuously.)
        let mut ok = false;
        for _ in 0..4_000 {
            cluster.run_for(Duration::from_millis(5));
            let h = cluster.roll_health_view(1);
            let synced = h["reasons"]
                .as_array()
                .is_some_and(|r| r.iter().all(|r| r["kind"] != "metadata_not_synced"));
            let hosted = cluster.hosted_tablets(1).contains(&tablet);
            if synced && !hosted {
                windows += 1;
                assert_eq!(
                    h["ok"], false,
                    "seed {seed}: node 1 has synced Metadata but hosts nothing, yet roll-health \
                     says it is safe to roll past: {h}"
                );
                assert!(
                    h["reasons"]
                        .as_array()
                        .is_some_and(|r| r.iter().any(|r| r["kind"] == "local_group_not_hosted")),
                    "seed {seed}: {h}"
                );
            }
            if h["ok"] == true {
                assert!(hosted, "seed {seed}: ok without hosting: {h}");
                ok = true;
                break;
            }
        }
        assert!(
            ok,
            "seed {seed}: never converged to ok: {}",
            cluster.roll_health_view(1)
        );
    }
    assert!(
        windows > 0,
        "the restarted node was never seen with synced Metadata and no hosted group: the test \
         no longer exercises the window"
    );
}
