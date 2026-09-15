# AWS's `BatchStatementErrorCodeEnum` (and similar bare per-item error-code enums) are their own naming scheme, not a mechanical `...Exception` suffix strip (2026-09-07, W-07 PR 4)

Every top-level DynamoDB error this adapter renders elsewhere carries an
`...Exception` `__type` suffix (`ConditionalCheckFailedException`,
`ResourceNotFoundException`, ...). `BatchExecuteStatement`'s own
per-statement `Error.Code` field uses a *different*, AWS-defined bare
enum (`BatchStatementErrorCodeEnum`) that looks at first glance like it's
just that same code with `Exception` stripped
(`ConditionalCheckFailedException` → `ConditionalCheckFailed`,
`DuplicateItemException` → `DuplicateItem`) — but one member breaks the
pattern: `ValidationException` maps to `ValidationError`, not
`Validation`. A mapping function that mechanically strips a
`...Exception` suffix instead of an explicit match table would have
silently produced a code AWS's own SDKs don't recognize for the single
most common per-statement failure (a parse error or the batch-only
exact-key restriction, both `ValidationException` internally). **General
lesson: when translating this adapter's own error taxonomy into a
*different*, AWS-defined bare-code enum for a sub-response shape (per-item
batch errors, per-action `CancellationReasons`, ...), write an explicit
match table and check every member against AWS's actual published enum
values — never assume a suffix-strip transform holds for the whole set
just because it holds for most of it.**
