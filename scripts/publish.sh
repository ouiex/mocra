#!/usr/bin/env bash
#
# Publish the mocra package to crates.io.
#
#   1. cargo login <your-crates.io-token>      # once (stored in ~/.cargo/credentials.toml)
#   2. ./scripts/publish.sh                     # DRY RUN (default) — validates, publishes NOTHING
#   3. DRY_RUN=0 ./scripts/publish.sh           # REAL publish — IRREVERSIBLE (you can only yank)
#
# Notes:
# - Your Cargo config replaces crates-io with a mirror (ustc / rsproxy); `cargo publish` refuses
#   that, so this script temporarily disables the `replace-with` lines and restores them on exit.
set -euo pipefail
cd "$(dirname "$0")/.."

DRY_RUN="${DRY_RUN:-1}"
case "$DRY_RUN" in
  0|1) ;;
  *) echo "DRY_RUN must be 0 or 1" >&2; exit 2 ;;
esac

backups=()
restore() { for f in "${backups[@]:-}"; do [ -e "$f.mocra-pub-bak" ] && mv -f "$f.mocra-pub-bak" "$f"; done; }
trap restore EXIT
for f in ".cargo/config.toml" "$HOME/.cargo/config.toml"; do
  if [ -f "$f" ] && grep -q 'replace-with' "$f"; then
    cp "$f" "$f.mocra-pub-bak"; backups+=("$f")
    perl -i -pe 's/^(\s*replace-with\s*=)/# disabled-for-publish: $1/' "$f"
    echo "  (temporarily disabled crates-io mirror in $f)"
  fi
done

if [ "$DRY_RUN" = "1" ]; then
  echo ">>> cargo publish -p mocra --dry-run"
  cargo publish -p mocra --dry-run
  echo "==> dry-run done."
else
  echo ">>> cargo publish -p mocra"
  cargo publish -p mocra
  echo "==> publish done."
fi
