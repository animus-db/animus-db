# A "should never happen, an earlier layer validates it" comment is a claim, not a guarantee (issue #846)

`AttributeValue::key_bytes`'s `N` arm had a fallback (raw-ASCII bytes) for
when `numkey::encode` fails, commented as existing "so a read path never
panics on data that somehow got here anyway, not because it is expected to
be hit" — i.e. the comment asserted the wire layer already validated `N`
text before it could ever reach this point. Nothing enforced that: the
`"N"` decode arm in `animus_dynamo::wire::decode_attribute_value` accepted
any JSON string with no format check, so `{"N":"12a"}` reached the
fallback from an ordinary `PutItem` and silently sorted a stored key by
raw text instead of by the order-preserving `numkey` encoding — corrupting
scan order not just for the bad row but for every well-formed numeric
neighbour sharing its partition, since `ScanIndexForward`'s whole guarantee
(ADR 0063) rests on every stored key using the same encoding.

**The general form**: a defensive fallback justified by "the real input
can't reach here, an earlier layer already checked" is only as sound as
that earlier layer's actual code, not its comment. Grep the layer the
comment names for the actual check before trusting a fallback's own
"unreachable in practice" framing — especially for a fallback that
silently produces a *different, wrong* result rather than erroring loudly,
since a silent wrong-but-plausible answer (mis-ordered keys here, not a
panic or an obvious error) is far more likely to go unnoticed in review
and in production than a crash would be. The fix closed the gap at the
actual validation boundary (a new `numkey::encode_checked`, layering
DynamoDB's 38-significant-digit cap that `encode` alone doesn't enforce on
top of `encode`'s own grammar check, called from the wire `"N"` arm) and
corrected the downstream comment to state the invariant as intended-but-
not-guaranteed, rather than as an established fact the type system
enforces.
