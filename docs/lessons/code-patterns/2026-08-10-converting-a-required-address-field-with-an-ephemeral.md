# Converting a required address field with an ephemeral-fallback default (`SocketAddr` + `#[serde(default = "default_ephemeral_addr")]`) to `Option<SocketAddr>` and reusing a bare `#[serde(default)]` silently changes what "missing from JSON" means — from "give it a working ephemeral value" to "this role isn't run here."

**Converting a required address field with an ephemeral-fallback default
(`SocketAddr` + `#[serde(default = "default_ephemeral_addr")]`) to
`Option<SocketAddr>` and reusing a bare `#[serde(default)]` silently changes
what "missing from JSON" means — from "give it a working ephemeral value"
to "this role isn't run here."** Splitting `animusd`'s `RoleAddrs.control`/
`raftkv` into per-role `Option<SocketAddr>` (ADR 0035 PR2 — a data-only node
has no `control` address, a control-only node has no `raftkv` address),
`#[serde(default)]` on `Option<T>` defaults a wholly-missing key to `None`,
not to the old ephemeral fallback — so an ancient config (predating the
`raftkv` field, hence always missing it, and also predating `role` so it
defaults to `Both`) would deserialize as "`Both`-role but no `raftkv`
address," an internally inconsistent state the actual entry points (`Node::
bind`) then reject as a hard error. Caught immediately by a same-PR back-
compat unit test built specifically to probe this case
(`oldest_json_shape_missing_optional_fields_loads`) rather than discovered
later against a real old config. Fix: give the field its own named default
function returning `Some(default_ephemeral_addr())`, so a wholly-absent key
still means "ephemeral, combined mode" while an explicit JSON `null` (only
ever written by a role-aware producer) still means `None`. **When narrowing
a field's type from `T` to `Option<T>` for a new "this doesn't apply"
case, re-derive what a *missing key* should mean — don't assume
`#[serde(default)]`'s blanket `None` matches the old defaulted-`T`
behavior; write a test that deserializes the actual old-shape JSON (not
just the new struct's `Default`) to prove it.** (`animusd::RoleAddrs`,
`config.rs`.)
