# A comment added "next to" a value inside a raw string becomes part of the value (2026-08-23).

**A comment added "next to" a value inside a raw string becomes part of the
value (2026-08-23).** While adding `"ConsistentRead":true,` to JSON request
bodies held in `r#"…"#` literals, one site got an explanatory `// ADR 0055 …`
appended on the same line — inside the raw string, so the request body
shipped a `//` comment as JSON and the edge answered `400` on every page.
It failed deterministically, which is the good case; the trap is that the
edit *looks* right in a diff, because a trailing `//` comment is exactly
what you would write one line earlier or later in real code. Rule: when
annotating a change inside a string literal, the comment goes **outside the
literal** (above the `format!`/`let`, or at the call site) — and grep the
touched files for a comment marker inside quotes (`'"[^"]*//'`) as a
mechanical post-check, the same way the `NAME:`-vs-`NAME::` entry below
recommends for path-shaped mangling.
