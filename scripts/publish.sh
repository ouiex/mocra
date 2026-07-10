#!/usr/bin/env bash
#
# Publish all mocra crates to crates.io in dependency order.
#
#   1. cargo login <your-crates.io-token>      # once (stored in ~/.cargo/credentials.toml)
#   2. ./scripts/publish.sh                     # DRY RUN (default) — validates, publishes NOTHING
#   3. DRY_RUN=0 ./scripts/publish.sh           # REAL publish — IRREVERSIBLE (you can only yank)
#
# Notes:
# - crates.io forbids path deps, so every internal dep is pinned `{ path = "...", version = "..." }`.
# - Your Cargo config replaces crates-io with a mirror (ustc / rsproxy); `cargo publish` refuses
#   that, so this script temporarily disables the `replace-with` lines and restores them on exit.
# - In DRY RUN, `mocra-core` and `mocra` are expected to fail with "can't find <dep> on crates.io"
#   because their deps aren't published yet — that's normal; the REAL run publishes leaves first so
#   each dependency is available in turn.
set -euo pipefail
cd "$(dirname "$0")/.."

DRY_RUN="${DRY_RUN:-1}"
LEAVES=(mocra-proxy mocra-dag mocra-store mocra-cluster)   # no internal deps
# then: mocra-core (needs the leaves), then the root crate `mocra` (needs core + proxy + cluster)

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

flags=""; [ "$DRY_RUN" = "1" ] && flags="--dry-run"
publish() {
  local target="$1"
  echo ">>> cargo publish $flags $target"
  cargo publish $flags $target
  [ "$DRY_RUN" = "1" ] || sleep 10   # give crates.io a moment to index before the next crate
}

for c in "${LEAVES[@]}"; do publish "-p $c"; done
publish "-p mocra-core"
publish ""            # root package: mocra

echo "==> ${DRY_RUN:+dry-run }done."
