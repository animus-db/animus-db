# Bound every decompression of untrusted input with `Read::take`

**Issue #1189.** `import.rs` `gunzip_bytes` ran `GzDecoder::read_to_string` on a
customer-supplied S3 object. A few-KB gzip can inflate to gigabytes, so one
hostile (or just oversized) import object could OOM the node.

**What to do.** Never `read_to_string`/`read_to_end` a decoder over data the
operator or a customer controls. Wrap it in `decoder.take(limit + 1)` and
treat `len > limit` as a named, distinct error; the `+ 1` is what tells "exactly
at the cap" from "overran it" without ever buffering more than `limit + 1`
bytes. Make the limit a parameter so a test can use a tiny cap against a
1 MiB high-ratio fixture instead of allocating the production cap. Decide
retry-vs-terminal per error class: an oversized object is content, so retrying
the identical bytes cannot help and the job should fail, unlike an I/O fault.
When no ADR documents the cap, derive it from documented per-item limits and the
writer's own layout, and say so next to the constant.
