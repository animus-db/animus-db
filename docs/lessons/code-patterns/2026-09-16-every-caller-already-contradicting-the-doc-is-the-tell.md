# Every caller's own comment already contradicting the producer's doc is the tell (issue #858)

`RaftKvNode::pending_changes`'s doc asserted its return order was **commit
order** (twice, citing an ADR section), because a change-log record's key
ends in its own commit HLC. True within one partition, false across the
whole tablet: the key is `token || escape(pk) || packed_hlc || ordinal`, so
a whole-scope sweep groups by partition (token) first and is HLC-ordered
only *within* one partition. Nobody had to discover this by reasoning about
murmur3 token order from scratch — every real caller's own comment already
said so, in nearly identical words, citing the true source (an ADR section
the producer's own doc never referenced): `seal_now`'s "`pending_changes`'
own key order is token-then-pk-then-HLC, NOT commit order (see its doc)"
sits two lines above a `sort_by_key` that would be dead code if the doc
were actually true.

**The general form**: when several independent call sites all carry a
comment that contradicts the API they're calling — especially when they
*cite* that API's own doc while disagreeing with it — that is stronger
evidence than the doc itself. Callers only write "despite what the doc
says" comments after being burned or after reading the actual key-
construction code; the producer's doc drifted because nobody's gate catches
a stale doc comment, while every caller's defensive re-sort kept working by
construction regardless of which claim was true. Grep every caller before
trusting a producer's ordering/uniqueness/liveness claim in its doc, not
just the producer's own implementation (contrast issue #846, where the
gap was between a comment and the *validating layer's* code — here every
signal needed was already sitting in sibling files, cross-referencing the
same doc).

**The fix technique, not just the fix**: correcting the prose closes the
immediate contradiction, but a future caller can still make the same wrong
assumption by not reading the doc closely enough. Renaming the method to
say what it actually returns (`pending_changes_key_order`) makes the
property visible at every call site's own text, permanently, rather than
depending on a doc comment nobody is forced to re-read. Prefer this for any
method whose name alone would otherwise invite a reasonable-sounding wrong
assumption about ordering, especially once at least one caller has already
had to write a "NOT X, see the doc" comment to correct that assumption —
that comment is itself the signal that the name is carrying the wrong
implication.
