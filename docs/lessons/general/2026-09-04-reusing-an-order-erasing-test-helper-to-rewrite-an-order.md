# Reusing an order-erasing test helper to rewrite an order-sensitive test produces a silently vacuous assertion (W-03 step 3, `dynamo_query_range.rs`)

Wiring `numkey` into `AttributeValue::key_bytes` (ADR 0063) flipped a test
that used to pin the *old*, wrong byte-text order for an `N` sort key
(`scan_index_forward_is_still_byte_order_not_numeric_order_for_n`) into one
asserting the *correct* numeric order
(`scan_index_forward_orders_n_sort_keys_numerically`). The fixture file
already had an `sk_values(body) -> Vec<String>` helper used by every other
test in the file — but its own doc comment said why: it ends with
`out.sort()` and is documented "order-independent, so callers assert
membership, not position." The first draft of the rewritten ordering test
called `sk_values` anyway (it was the only extractor in scope, and its name
looked right), asserting `sk_values(&asc) == vec![...expected numeric
order...]`. That assertion would have passed **unconditionally** — sorting
the extracted strings erases the exact property (result *order*) the test
exists to check, so a regression back to byte-text order, or any other
ordering bug entirely, would never fail it. Caught before commit only by
rereading the helper's own doc comment, not by the test failing (it didn't
— it can't).

**Fixed** by adding a second, order-preserving helper (`sk_order`, the
first-appearance-order extraction `sk_values` already did internally before
its own final `.sort()`) and using that for the position-sensitive
assertions, keeping `sk_values` for the file's other, genuinely
membership-only assertions.

**General form**: when a fixture file's existing helper is documented as
order-erasing (a trailing `.sort()`, a `BTreeSet`/`HashSet` collect, a
membership-only comparison), that documentation is not incidental — it is
recording a *narrower contract than the function's name suggests*, and
reusing it for a differently-shaped assertion (position/order rather than
membership) produces a test that type-checks, reads naturally, and can
never fail regardless of the code under test. Before reusing any helper to
assert an order-sensitive property, re-read its own doc/implementation for
exactly this kind of narrowing, and prefer writing a fresh order-preserving
extractor (as this fix did) over trusting that a same-shaped helper already
in scope must mean what its name implies.
