//! Elle-style history recorder and consistency checker for CustosDB.
//!
//! Records a [`History`] of `invoke`/`ok`/`fail`/`info` operations over a
//! list-append datatype with virtual-time stamps, then checks it:
//!
//! - [`check_cycles`] — serializability of the transactional path, via a
//!   dependency-graph cycle search (the core idea behind Elle).
//! - [`check_durability`] / [`check_convergence`] — the AP path: acknowledged
//!   writes are not lost, and independent final reads agree.
//!
//! Histories [`export`] to JSON or Jepsen/Elle EDN for offline analysis. A
//! checker's [`CheckReport`] carries the run's seed so a flagged anomaly is
//! replayable.

pub mod check;
pub mod export;
pub mod history;

pub use check::{CheckReport, check_convergence, check_cycles, check_durability};
pub use history::{Entry, History, Key, ListVal, Mop, Outcome, Process, Recorder};
