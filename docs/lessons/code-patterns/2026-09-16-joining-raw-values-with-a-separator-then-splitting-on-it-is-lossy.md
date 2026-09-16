# Joining raw (unescaped) values with a separator and later re-splitting on that separator is lossy the moment a value can contain it

Issue #855: `animus-s3`'s SigV4 query builder joined raw `(key, value)`
pairs into a single string with `format!("{k}={v}")` + `.join("&")`, then
recovered the pairs downstream by `split('&')` + `split_once('=')` — once to
sign the request, once to build the wire URI. Both derivations started from
the *same* corrupted string, so a value containing a literal `&` (a
customer-supplied `S3KeyPrefix`, which S3 explicitly permits to contain
`&`) was truncated at the first `&` and the remainder reappeared as a
spurious extra query parameter — self-consistently signed either way, so
nothing detected it; S3 just interpreted the request differently than
intended. The crate's own `fake::FakeS3` test double had the identical bug
on the receiving side: it percent-decoded a whole wire query string first
(turning `%26` back into `&`) and *then* split the decoded string on `&`,
which is the same information loss in reverse.

**The general lesson: once several raw values are joined into one string
with a separator that could also occur inside a value, there is no way to
losslessly recover the original values by splitting on that separator
again.** This holds regardless of which "direction" the join happens in a
pipeline:

- Building outbound: don't join raw pairs into a delimited string as an
  intermediate representation at all. Carry the pairs as a typed
  collection (`&[(&str, &str)]`) all the way to wherever they get
  percent-encoded, and encode+join **once**, at the very last step, from
  the original raw pairs — never re-parse your own joined output.
- Parsing inbound: if the incoming string is guaranteed **already escaped**
  (e.g. a real HTTP query string, where a value's own `&`/`=` must already
  be `%26`/`%3D`), splitting on the raw separator *before* decoding is
  safe by construction — decoding first and splitting second reopens the
  exact same bug, because decoding removes the very escaping that made the
  separator unambiguous.

Fixed in `crates/animus-s3/src/sigv4.rs` (`canonical_query_string` now
takes `&[(&str, &str)]` directly; `RequestToSign::query` is typed pairs,
not a joined string) and `crates/animus-s3/src/fake.rs` (`FakeS3` now
parses the wire query with `split`-before-`decode`, never `decode`-then-
`split`). See `crates/animus-s3/CLAUDE.md`'s "Encode exactly once" section
for the worked-through account of both directions.

**When reviewing or writing any code that builds a delimited string from
several dynamic values and later needs those values back apart, ask: can
any value contain the delimiter? If the answer isn't a hard structural
"no" (not "unlikely," not "not today"), the join-then-split round trip is
already broken — even if every test you'd think to write for it happens to
use values without the delimiter in them.**
