//! `animus-bench`: an open-loop, coordinated-omission-corrected load
//! generator that drives an AnimusDB cluster **as a client** over the real
//! SigV4-signed DynamoDB wire (B-01). See this crate's `CLAUDE.md` for the
//! design, the library API and how to add a scenario.
//!
//! Layering (the first six modules are pure and clock-free; only [`rt`],
//! [`client`], [`cluster`], [`engine`] and [`ycsb`] touch real I/O):
//!
//! - [`schedule`] — intended send times;  [`recorder`] — HDR histograms,
//!   corrected + service time;  [`dist`] — zipfian / uniform / latest;
//!   [`workload`] — YCSB A-F op streams;  [`report`] — the results document.
//! - [`engine`] — `run_phase`, the thin open-loop I/O shell;
//!   [`scenario`] — warm-up/steady/sweep and baseline/degraded/recovery
//!   plans plus the YCSB glue;  [`cluster`] — the system under test and its
//!   fault hooks;  [`envinfo`] — host + `/admin` topology capture;
//!   [`compare`] — pure A/B comparison of results files (`animus-bench compare`).

pub mod cli;
pub mod client;
pub mod cluster;
pub mod compare;
pub mod dist;
pub mod engine;
pub mod envinfo;
pub mod recorder;
pub mod report;
pub mod rt;
pub mod scenario;
pub mod schedule;
pub mod workload;
pub mod ycsb;
