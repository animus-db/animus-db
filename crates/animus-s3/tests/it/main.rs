//! Single merged integration-test binary for this crate (one link instead of 4).
//! Real-thread / `ProdEnv` binaries stay separate `tests/*.rs` targets — see the crate's CLAUDE.md.

mod client_fake;
mod credentials;
mod multipart;
mod query_encoding;
mod sigv4_chain_matches_dynamo;
mod sigv4_known_answers;
