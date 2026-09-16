# Don't hardcode an external "known answer" test value you cannot independently verify — a wrong memorized constant is a worse oracle than no test at all (S-04 PR 1, `animus-s3`)

The task brief for `animus-s3`'s SigV4 signer named a specific known-answer
case to include: AWS's own published S3 "GetObject" SigV4 worked example
(`GET /test.txt`, access key `AKIAIOSFODNN7EXAMPLE`), with a specific
expected final `Signature` value. Reconstructing the exact request that
example signs from memory alone (no network access in this sandbox, no
vendored copy of that specific worked example anywhere in the local
toolchain or registry cache) — several independently plausible
byte-for-byte reconstructions (with/without a `Range` header, with/without
`x-amz-content-sha256` in the signed-header set) — none reproduced the
stated signature, even though the same HMAC chain independently reproduces
every vendored `aws-sig-v4-test-suite` vector byte-for-byte (proving the
*algorithm* is correct) and independently reproduces the worked example's
own commonly-quoted intermediate `StringToSign` hash. The conclusion: the
exact canonical request bytes for that specific example were misremembered
somewhere in the reconstruction, not that the code was wrong — but there
was no way to tell which, without an authoritative source to check against.

**The decision made instead of guessing**: drop the hardcoded external
constant rather than ship a test asserting a value that cannot be
independently verified in the environment building it. Pinning a
plausible-but-unverified memorized value as a "known answer" regression
test is actively worse than not having one — a future mismatch would say
nothing about whether the *code* regressed, only whether the *test's own
memorized oracle* was ever right in the first place, and a lucky
coincidental match would prove nothing either. The test that shipped
instead checks what's actually self-verifiable without an external
oracle: the request's own `SignedHeaders` shape, and that the signer's own
output round-trips through the same crate's own independent verifier
function — plus a cross-crate equivalence test against `animus_dynamo`'s
already-shipped, independently-implemented SigV4 chain (which *is*
verified, against the vendored AWS test suite, in that crate's own test
suite). Both are documented in place as deliberate, with the reasoning for
why, rather than silently substituting a weaker test with no explanation.

**General form**: when a task brief hands you a specific external
"known-good" value to test against and you cannot independently verify the
exact input that produces it (no network access, no vendored fixture, no
authoritative local copy) — reconstructing it from memory and asserting
equality anyway is a coin flip dressed as a regression test. Verify what
you can with tools you actually have (vendored fixtures, independently-
reimplemented cross-checks, self-consistency round-trips), state plainly
what you could not verify and why, and let the person who can reach a
network/the authoritative source fill in the one assertion you couldn't
make honestly.
