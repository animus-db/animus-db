# When the vendor docs cannot be fetched, cite search extracts and list what is unverified

Writing ADR 0075 (global tables) required AWS semantics the roadmap had only
"recalled". The sandbox egress proxy blocked `docs.aws.amazon.com` for direct
fetches, but web search returned page extracts. The workable method: record each
fact with the page URL it surfaced under, label the ADR's verification as
"search extracts, not full pages", keep an explicit *Not verified* list, and make
the decisions robust to those items (or flag where they are not). Searching also
caught a live contradiction (a 2026-09 AWS announcement relaxing the MRSC
region-set rule that the developer guide still stated) that recalled knowledge
would have missed. Do not assert an unread page; do not silently drop the claim
either.
