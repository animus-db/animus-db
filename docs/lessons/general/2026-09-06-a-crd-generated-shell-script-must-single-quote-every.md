# A CRD-generated shell script must single-quote every operator-controlled string value that becomes a command-line argument

`animus-operator`'s `entrypoint.sh` generator (`desired::cluster_config::
entrypoint_script`) started passing `s3://...` URIs (`spec.s3.backupStore`/
`segmentStore`, S-04 PR 3) as literal `--backup-store`/`--segment-store`
arguments in the generated POSIX `sh` script. An S3 URI's own query string
always contains `&` (separating query parameters) and usually `?`/`=` —
every one of `&`, `?` unquoted in `sh` is either a metacharacter (`&`
backgrounds the preceding command entirely, silently turning `exec
animusd ... --backup-store s3://bucket?a=1&b=2` into two separate
commands) or at minimum a portability risk. Every other flag value this
generator already emitted (paths, ports) happened to be safe unquoted, so
this was the first flag value here that actually needed it. **General
form**: any generator that interpolates a user- or spec-controlled string
into a shell script (not just this one) must single-quote (with the
standard `'...'` → `'\''` embedded-quote escape) every such value at the
point of interpolation, not just the values that are "obviously" URLs or
paths — the generator has no way to know in advance which future field
will be the first one containing a shell metacharacter, and getting this
wrong doesn't fail at generation time, only at container start, in a
place `bash -n` (syntax-only) also won't catch since `&` is syntactically
valid shell, just semantically wrong here.
