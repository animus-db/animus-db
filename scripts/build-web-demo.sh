#!/usr/bin/env bash
# Build the in-browser demo into website/demo/ (gitignored; ADR 0082): the
# WebAssembly into demo/pkg/ and the two real animusd web UIs (the data console and
# the operator dashboard) into demo/apps/. Used by .github/workflows/pages.yml and for local previews:
#
#   scripts/build-web-demo.sh && python3 -m http.server 8000 --directory website
#
# Needs: the rust toolchain with the wasm32-unknown-unknown target
# (`rustup target add wasm32-unknown-unknown`) and a `wasm-bindgen` CLI whose
# version equals the `wasm-bindgen` crate in Cargo.lock
# (`cargo install wasm-bindgen-cli --version <that> --locked`). A mismatch is a
# hard error in wasm-bindgen itself; this script checks it first for a clearer
# message. Set CARGO_TARGET_DIR to keep the wasm build out of ./target.
set -euo pipefail
cd "$(dirname "$0")/.."

# The data console and the operator dashboard run in the demo page unmodified. The
# single source of truth is crates/animusd/src: copy the files, concatenating the
# stylesheet the same way console.rs / dashboard.rs do (fonts, then tokens, then the
# UI's own). website/assets/demo-frame.js loads them.
src=crates/animusd/src
rm -rf website/demo/apps
mkdir -p website/demo/apps/console website/demo/apps/dashboard
cp "${src}/console.html" website/demo/apps/console/index.html
cp "${src}/console.js" website/demo/apps/console/console.js
cat "${src}/fonts.css" "${src}/tokens.css" "${src}/console.css" > website/demo/apps/console/console.css
cp "${src}/dashboard.html" website/demo/apps/dashboard/index.html
cp "${src}"/dashboard_*.js website/demo/apps/dashboard/
cat "${src}/fonts.css" "${src}/tokens.css" "${src}/dashboard.css" > website/demo/apps/dashboard/dashboard.css

want=$(awk '/^name = "wasm-bindgen"$/ {getline; gsub(/[^0-9.]/, "", $0); print; exit}' Cargo.lock)
have=$(wasm-bindgen --version 2>/dev/null | awk '{print $2}' || true)
if [ "${want}" != "${have}" ]; then
  echo "wasm-bindgen CLI is '${have:-missing}', Cargo.lock pins ${want}:" >&2
  echo "  cargo install wasm-bindgen-cli --version ${want} --locked" >&2
  exit 1
fi

cargo build -p animus-web --profile wasm-release --target wasm32-unknown-unknown --locked
wasm="${CARGO_TARGET_DIR:-target}/wasm32-unknown-unknown/wasm-release/animus_web.wasm"

rm -rf website/demo/pkg
wasm-bindgen --target web --out-dir website/demo/pkg \
  --remove-name-section --remove-producers-section "${wasm}"

raw=$(wc -c < website/demo/pkg/animus_web_bg.wasm)
gz=$(gzip -9 -c website/demo/pkg/animus_web_bg.wasm | wc -c)
echo "website/demo/pkg/animus_web_bg.wasm: ${raw} bytes raw, ${gz} bytes gzip"
