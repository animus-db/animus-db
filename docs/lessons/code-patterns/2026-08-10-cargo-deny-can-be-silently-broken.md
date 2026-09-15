# `cargo deny` can be silently broken

**`cargo deny` can be silently broken** (e.g. the repo's own `AGPL-3.0-only`
missing from the allow-list) and it can't run in every local env — CI runs it;
treat it as a real gate, not optional.
