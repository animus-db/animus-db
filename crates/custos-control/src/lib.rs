//! Control plane: a Raft-replicated metadata state machine holding membership
//! and the tablet map, with compare-and-swap (epoch) transactions (see
//! `docs/adr/0001-two-plane-architecture.md`).
//!
//! Populated in milestone M3.
