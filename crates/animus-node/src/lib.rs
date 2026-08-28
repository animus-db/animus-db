//! AnimusDB's `E: Env`-generic node core (ADR 0061 Decision 1, Phase C
//! carve-out). This is the growing home of every piece of node logic that
//! needs no real clock, no real socket, and no `tokio` — the layer that
//! `animusd`'s own `CLAUDE.md` and the root `CLAUDE.md`'s "load-bearing
//! constraint" section describe, but carved into its own crate so the
//! boundary is **compiler-enforced** instead of review-enforced: this
//! crate's manifest depends on `animus-env` with `default-features =
//! false` (so `ProdEnv`/`FsSegmentStore` are not merely unused, they are
//! not even *compiled* into this crate's build) and does not depend on
//! `tokio` at all. A `ProdEnv::new(..)` or a `tokio::net::TcpStream` here is
//! a build failure, not a review miss. See this crate's own `CLAUDE.md` for
//! the full rationale and what is still to move here in later rungs.
//!
//! **Rung C1 (this crate's first slice)** moved the pure, `Env`-free
//! surface only: the client-facing wire types ([`wire`] — `ClientRequest`/
//! `ClientResponse`/`Surface`/`is_relayable_command` and the plain-data
//! types they embed), [`topology`] (CP-route resolution), and [`decide`]
//! (the pure decision predicates lifted out of `animusd::ClientCtx` by ADR
//! 0061 rung A6). `animusd` re-exports everything here at its own crate
//! root so its ~500 existing call sites keep compiling unchanged; see that
//! crate's `CLAUDE.md` for the re-export shim and what has NOT moved yet
//! (`ClientCtx`, `handle_request`, `cp_serve_forwarded` — rung C5).

pub mod backup_completion;
pub mod backup_janitor;
pub mod decide;
pub mod host;
pub mod index_backfill;
pub mod topology;
pub mod ttl_reaper;
mod wire;

pub use wire::{
    ClientRequest, ClientResponse, KindWriteOp, PendingKindWrite, Surface, TxnPrecondition,
    TxnTableWrite, TxnWriteCondition, is_relayable_command, surface_of,
};
