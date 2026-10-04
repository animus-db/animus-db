# S3 `CompleteMultipartUpload` can answer 200 and fail in the body

**Found while:** S-08 M2 (multipart upload).

S3 flushes the `200 OK` headers of `CompleteMultipartUpload` early (to keep
the connection alive during assembly) and, if assembly then fails, puts an
`<Error>` document in the body of that same 200. A client that maps by status
code alone reports a successful upload of an object that does not exist.
`animus_s3::xml::parse_complete_multipart` therefore requires a
`CompleteMultipartUploadResult` and treats any `<Error>` as failure;
`InternalError`/`SlowDown` inside a 200 are re-surfaced as a 5xx-shaped
`Service` error so the caller's existing 5xx retry applies. `FakeS3`
reproduces the shape (`set_complete_error_in_200`) so the path is covered
without a real endpoint.

General rule: for any S3 call whose response is assembled server-side after
the headers (complete, copy), "success" is a property of the body, not the
status line.

Related testing gotcha: a store-level multipart test must size its payload
above the configured threshold (a payload at or below it silently takes the
single-PUT path and the "must fail" assertion passes the wrong way round).
