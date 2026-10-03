//! Single merged integration-test binary for this crate (one link instead of 17).
//! Real-thread / `ProdEnv` binaries stay separate `tests/*.rs` targets — see the crate's CLAUDE.md.

mod lsm_approx_bytes;
mod lsm_clone;
mod lsm_clone_filtered;
mod lsm_crash;
mod lsm_crash_encrypted;
mod lsm_disk_faults;
mod lsm_gc;
mod lsm_group_commit;
mod lsm_maintenance;
mod lsm_merge_fast_path;
mod lsm_metrics;
mod lsm_options_validation;
mod lsm_scan_range_gate;
mod lsm_semantics;
mod lsm_wal_rotation;
mod lsm_wal_sync_markers;
mod storage_basic;
mod storage_props;
