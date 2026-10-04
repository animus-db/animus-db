#!/usr/bin/env bash
# Populate fuzz/corpus/<target>/ from the golden format fixtures
# (crates/*/tests/fixtures/formats/, mapped by fuzz/seeds.tsv), a few slices
# derived from them, and the hand-written text seeds in fuzz/seeds/. The corpus
# directory is git-ignored: fixtures are read in place and copied here only on
# the machine that fuzzes, never checked in twice.
#
# Idempotent: rerun after adding a fixture or a seed. Runs on stable.
set -euo pipefail
cd "$(dirname "$0")"
exec cargo run --quiet --release --example seed_corpus
