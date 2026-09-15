# A numeric-vs-byte comparison fix applied to one predicate over a type doesn't automatically reach a sibling predicate over the same type (2026-08-24, issue #373, `animus-dynamo::condition`).

**A numeric-vs-byte comparison fix applied to one predicate over a type
doesn't automatically reach a sibling predicate over the same type
(2026-08-24, issue #373, `animus-dynamo::condition`).** This crate already
had the *correct* pattern for DynamoDB `N`: `compare_values`/`compare_numeric`
compare number text by decimal magnitude, with a doc comment explicitly
contrasting that against `AttributeValue::key_bytes`'s lexicographic
ordering (a documented simplification of on-disk key order, not something a
filter should inherit). But `SortKeyCondition::matches` — a second, older
predicate over the exact same `N` values, for `Query`'s `sk BETWEEN`/`sk =`
— had never been switched over, and kept comparing raw `key_bytes`
directly: `sk BETWEEN 5 AND 15` excluded `sk = 9` because `"15" < "9"` as
text. The general check: when a type has more than one comparison/equality
call site in a crate (here, `ConditionExpression`'s comparators and
`SortKeyCondition`'s own), a numeric-ordering fix to one is a signal to grep
every other site over the same type for the identical byte-vs-numeric
divergence, not just extend the one you're already touching — the existing
doc comment even named the divergence (`AttributeValue::key_bytes`'s
lexicographic number order) but that cross-reference apparently never got
checked against every reader of `key_bytes`, only the one being written at
the time.
