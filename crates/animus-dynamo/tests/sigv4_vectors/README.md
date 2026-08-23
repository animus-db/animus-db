# Vendored SigV4 test vectors

This directory vendors AWS's official Signature Version 4 test-vector suite
(the `aws-sig-v4-test-suite`, published as part of the AWS General Reference
SigV4 documentation), used by `../sigv4_vectors_test.rs` to validate
`animus_dynamo::sigv4` (ADR 0057) against an independent, non-Anthropic
oracle.

Fetched from the `mhart/aws4` GitHub mirror of the suite (the upstream AWS
docs repo does not expose the raw `.req`/`.creq`/`.sts`/`.authz` fixture
files individually over HTTP; this mirror carries them verbatim):

```
https://raw.githubusercontent.com/mhart/aws4/master/test/aws-sig-v4-test-suite/<case>/<case>.{req,creq,sts,authz}
```

Each case directory holds four files:

- `<case>.req` — the raw HTTP request (method/path/query, headers, and body
  after a blank line). No trailing newline.
- `<case>.creq` — the expected SigV4 canonical request string.
- `<case>.sts` — the expected string-to-sign.
- `<case>.authz` — the expected `Authorization` header value.

All cases use the suite's well-known fixed constants: access key id
`AKIDEXAMPLE`, secret `wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY`, date
`20150830T123600Z`, region `us-east-1`, service `service`.

`normalize-path/` holds the suite's path-normalization sub-cases
(`get-relative`, `get-slash`, `get-space`, etc.) — worth calling out
separately because `get-space` exists **only** under `normalize-path/`, not
as a top-level case (the suite's own layout, not a vendoring omission here).

Not vendored: `post-sts-token` (STS session-token requests — out of scope,
ADR 0057 supports only static credentials and does not validate
`X-Amz-Security-Token`) and any of the suite's other STS/presigned-URL cases
for the same reason.
