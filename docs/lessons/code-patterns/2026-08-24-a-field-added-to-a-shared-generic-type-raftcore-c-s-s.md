# A field added to a shared, generic type (`RaftCore<C, S>`'s `LogEntry`/ `RaftMsg`) needs a matching update at every *hand-rolled* encoder for it, not just wherever `#[derive(Serialize, Deserialize)]` already covers it — the "grep every gating match site" lesson (root `CLAUDE.md`) applies to a codec's field list exactly as much as to a command enum's match arms (2026-08-24, ADR 0058 Train 1's `learners` field).

**A field added to a shared, generic type (`RaftCore<C, S>`'s `LogEntry`/
`RaftMsg`) needs a matching update at every *hand-rolled* encoder for it,
not just wherever `#[derive(Serialize, Deserialize)]` already covers it —
the "grep every gating match site" lesson (root `CLAUDE.md`) applies to a
codec's field list exactly as much as to a command enum's match arms
(2026-08-24, ADR 0058 Train 1's `learners` field).** `LogEntry`/
`RaftMsg::InstallSnapshot` pick up `#[serde(default)]` handling for free
from `animus-control`'s own `serde_json`-based WAL/wire path, but
`animus-cp-data::codec.rs` is a **second, independent, hand-rolled binary
encoder** for the identical types (ADR 0017 A.2, built to avoid
`serde_json`'s ~3-4x `Vec<u8>` blowup) — its `put_entry`/`read_entry` and
`InstallSnapshot` arms enumerate every field explicitly, by hand, with no
compiler-enforced exhaustiveness the way a `match` on an enum has. A new
struct field added to `LogEntry`/`RaftMsg` compiles cleanly against this
codec with the field simply *never encoded* — no error, no warning, just a
silent drop the moment any message carrying it crosses the wire this codec
serves (which is every data-plane Raft message; `animus-control`'s own
`serde_json` WAL path is unaffected, since it never goes through this
codec at all). **General rule**: when a type has more than one encoder
(a derive-based one and a hand-rolled "compact wire format" one, or any
other duplicate-by-design serialization), a new field's checklist must
name *every* encoder explicitly — a derive macro's own exhaustiveness
cannot protect a sibling encoder it doesn't know exists. Caught here only
because the codec's own round-trip test (`every_wire_variant_round_trips`)
was updated deliberately as part of the same change, not because anything
would have failed on its own.
