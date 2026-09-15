# A regex mass-edit over Rust must be brace-balanced, not next-delimiter-based (2026-08-23).

**A regex mass-edit over Rust must be brace-balanced, not
next-delimiter-based (2026-08-23).** Adding `stale: false,` to every
`ClientRequest::Get { … }` literal across the test tree with
`re.sub(r"ClientRequest::Get \{\n(?:.*\n)*?( *)\},", …)` silently
corrupted two distinct shapes: a struct literal that ends in `};` (a `let`
binding) let the match run past it into an *unrelated* later literal —
`SplitTablet` acquired a `stale` field it has no business having — and a
literal whose last field had no trailing comma produced
`table: "kv".to_string()\n    stale: false,`. Both were caught only by the
build. The reliable shape is to find the opening `{` and walk forward
counting braces to its real partner, then insert relative to *that*; and to
check the preceding token for a comma before appending a field. This is the
same family as the 2026-08-22 `NAME:` vs `NAME::` entry below/above — a
scripted edit's blast radius is whatever its pattern matches, so prefer
patterns that can't run past the construct they name.
