#!/usr/bin/env bash
# animusd sets `autotests = false` (ADR 0082: its integration tests need the
# `server` feature), so a `crates/animusd/tests/*.rs` file that is not listed as
# a `[[test]]` in crates/animusd/Cargo.toml is silently never built or run.
# This fails if any such file is unlisted.
set -euo pipefail
cd "$(dirname "$0")/.."

manifest=crates/animusd/Cargo.toml
missing=0
for f in crates/animusd/tests/*.rs; do
  if ! grep -Fq "path = \"tests/$(basename "$f")\"" "$manifest"; then
    echo "::error file=${manifest}::${f} is not listed as a [[test]] in ${manifest} (autotests = false)" >&2
    missing=1
  fi
done
if [ "${missing}" -ne 0 ]; then
  echo "Add a [[test]] entry (name, path, required-features = [\"server\"]) for each file above." >&2
  exit 1
fi
echo "every crates/animusd/tests/*.rs is listed in ${manifest}"
