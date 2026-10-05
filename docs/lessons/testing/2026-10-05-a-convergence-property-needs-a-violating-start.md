# A convergence property needs a start state that violates the invariant

`replan_pinned` (region-pinned placement) is only meaningful if it repairs a
tablet that *already* breaks its pin, not just keeps a compliant one compliant.
A proptest that generates only valid starting sets passes against a no-op. The
property test therefore starts from skewed/duplicated-domain sets, asserts
convergence to a compliant set, a fixpoint on re-application, and permutation
stability, and a separate case asserts that a region with no node yields no
command (never repair across regions) instead of a best-effort fallback.
