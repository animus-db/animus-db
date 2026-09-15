# `prop_assert!`/`prop_assert_eq!`'s message argument goes through `concat!` internally, so it cannot implicitly capture identifiers the way a normal `format!("{x:?}")` can

**`prop_assert!`/`prop_assert_eq!`'s message argument goes through
`concat!` internally, so it cannot implicitly capture identifiers the way
a normal `format!("{x:?}")` can** — `prop_assert_eq!(got, want,
"compare_numeric({a:?}, {b:?})")` fails to compile with "there is no
argument named `a`" even though `a` is a real local in scope, because
proptest's macro expansion routes the string through `concat!` before it
ever reaches `format_args!`. Pass captured values as explicit trailing
positional arguments instead (`"compare_numeric({:?}, {:?})", a, b`) —
this is proptest-macro-specific, not a general Rust `format!` limitation,
so it only bites inside `proptest!{ .. }` bodies. (ADR 0061 rung A5,
`crates/animus-dynamo/src/condition.rs::decimal_differential_tests`.)
