//! Single merged integration-test binary for this crate (one link instead of 12).
//! Real-thread / `ProdEnv` binaries stay separate `tests/*.rs` targets — see the crate's CLAUDE.md.

mod backfill_fault_corpus;
mod backup_fault_corpus;
mod cycle_checker;
mod export_import_fault_corpus;
mod negative_control;
mod pitr_fault_corpus;
mod raftkv_linearizable;
mod segment_store_encrypted_fault_corpus;
mod stream_lineage_corpus;
mod txn_serializable;
mod upgrade_restart_corpus;
mod upgrade_restart_tier0;
mod upgrade_restart_txn_envelope;
