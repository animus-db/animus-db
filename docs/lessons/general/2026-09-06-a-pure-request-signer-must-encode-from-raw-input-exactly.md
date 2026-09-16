# A pure request signer must encode from RAW input exactly once per purpose, never re-encode an already-encoded string (S-04 PR 1, `animus-s3`)

Building `animus-s3`'s SigV4 signer/client, the first draft percent-encoded
an S3 object key at the client's own call site (`format!("/{bucket}/{}",
encode_key_path(key))`, building a wire-ready path up front) and then
handed that already-escaped string to `sigv4::RequestToSign::uri`, which
the canonical-request builder (`canonical_uri_s3`) percent-encodes AGAIN
internally. A key containing a space would sign against `%2520` (the `%`
of a real `%20` re-escaped) while the actual wire path sent `%20` —
guaranteed signature mismatch for any key needing escaping at all,
silently correct only for keys made entirely of unreserved characters
(exactly the case every early hand-written test happened to use). The
identical bug existed for the query string half (a hand-built,
already-percent-encoded query string handed to a function whose contract
is "raw in, canonical out"). Caught by adding one test with a key
containing a space, `+`, and parentheses (`a_key_containing_special_
characters_round_trips` in `crates/animus-s3/tests/client_fake.rs`) — no
existing test exercised anything but alphanumeric keys.

**The fix, and the general rule**: thread the RAW (unescaped) string all
the way from construction (`client::S3Client`'s own methods) to the
signer's input type, and derive each of the two representations that
actually need percent-encoding — the wire URI/query, and the signed
canonical form — **independently, by calling the encoding function once
each on the same raw input**, never by feeding one encoded output into the
other's encoder. This composes correctly by construction (the function is
pure and deterministic, so two independent calls on the same input always
agree) and needs no "is this string already encoded?" bookkeeping anywhere.
The mirror-image version of the same bug shows up on a **verifier** (this
crate's own `fake::FakeS3`, which receives an already-canonical wire
URI/query and must reconstruct the canonical form to check a signature
against): the fix there is `percent_decode` once, recovering a raw string,
before handing it to the same canonicalization function real signing uses
— never re-canonicalizing the wire bytes directly. Any code that both
signs/builds a request AND independently reconstructs/verifies one (which
describes every SigV4 signer-plus-verifier pair, and generalizes to any
"canonicalize a string for one purpose, transmit a related-but-different
representation of it for another" design) should state, in one place,
which representation ("raw" vs. "canonical") each function's input/output
contract expects — and grep every call site for a mismatch before trusting
a test suite that happens to only exercise inputs where the bug is
invisible (plain ASCII, no reserved characters).
