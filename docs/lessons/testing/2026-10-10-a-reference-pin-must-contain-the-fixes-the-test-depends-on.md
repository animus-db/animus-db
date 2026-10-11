# A cross-version reference pin must contain every fix the test's workload depends on

A previous-release test pinned to an old commit (`ac57d56a`) kept a whole workload
class (multi-key transactions) switched off for five days, because the pin itself
had a bug that the current tree had already fixed (`efcaa6cb`). The failures looked
like roll bugs (#1237, #1238) until the `same-binary` control showed the reference
losing writes with no upgrade at all.

- Run the control (`ANIMUS_UPGRADE_FROM_CONTROL=same-binary`) first when a roll
  fails: if the reference alone fails, the pin is the defect, not the roll.
- When choosing a pin before any release tag exists, take the oldest CI-green tree
  that contains the fixes the workload needs and still matches the test's
  assumptions (here: no version era started in production).
- A shallow clone cannot resolve such a pin; use a full clone (CI uses
  `fetch-depth: 0`) and check `git merge-base --is-ancestor <fix> <pin>`.
