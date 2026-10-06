# A version floor above 1 stops a fresh cluster from ever starting its era

`MIN_SUPPORTED` was defined as `MAX_SUPPORTED - 1` (ADR 0073's N-1/N skew), and the
G-d plan said it "becomes 2 automatically" with `MAX = 3`. But the era starts at
cluster version 1 (the first applied `ReportNodeVersion`) and `Metadata::apply`
rejects a report whose range excludes the *current* cluster version. A binary
advertising `[2, 3]` therefore can never report into a fresh cluster at version 1:
the era never starts. Reading the apply arm before accepting a "this follows
automatically" line in a plan found it; the unit test `floor_formula_...` had
asserted the formula, which is exactly why it would have passed while the real
bootstrap broke.

Rule: hold `MIN_SUPPORTED` at 1 until the era-start rule is redesigned (document it in
the const and the ADR), and when a plan says a constant changes "automatically", find
the apply/startup check that consumes it. The same bump makes any gate whose version
is `<= MIN_SUPPORTED` a no-op, which `Gate::ALL`'s `v > MIN_SUPPORTED` test would also
have flagged for `GlobalTables`.
