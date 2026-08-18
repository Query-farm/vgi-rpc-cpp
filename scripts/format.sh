#!/usr/bin/env bash
# Format every hand-written C++ file, or check that it is already formatted.
#
#   scripts/format.sh          # rewrite in place
#   scripts/format.sh --check  # exit 1 and name the files that differ (CI)
#
# The version is pinned because clang-format's output changes between major
# releases: two contributors on different versions would reformat each other's
# files on every commit, which is the problem a formatter exists to solve.

set -uo pipefail

REQUIRED_MAJOR=22
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

find_clang_format() {
  for candidate in "${CLANG_FORMAT:-}" clang-format-$REQUIRED_MAJOR \
                   /opt/homebrew/opt/llvm/bin/clang-format \
                   /usr/local/opt/llvm/bin/clang-format clang-format; do
    [[ -z "$candidate" ]] && continue
    command -v "$candidate" >/dev/null 2>&1 || [[ -x "$candidate" ]] || continue
    local version
    version="$("$candidate" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
    [[ "${version%%.*}" == "$REQUIRED_MAJOR" ]] && { echo "$candidate"; return 0; }
  done
  return 1
}

CF="$(find_clang_format)" || {
  echo "clang-format $REQUIRED_MAJOR not found." >&2
  echo "  macOS: brew install llvm      (then /opt/homebrew/opt/llvm/bin/clang-format)" >&2
  echo "  pip:   pip install 'clang-format~=$REQUIRED_MAJOR.0'" >&2
  echo "  Or set CLANG_FORMAT=/path/to/clang-format." >&2
  exit 2
}

# Vendored vcpkg is excluded — it is not ours to format.
mapfile -t FILES < <(cd "$ROOT" && git ls-files '*.cpp' '*.h' '*.hpp' | grep -v '^vcpkg/')

if [[ "${1:-}" == "--check" ]]; then
  failed=0
  for f in "${FILES[@]}"; do
    if ! "$CF" "$ROOT/$f" | diff -q - "$ROOT/$f" >/dev/null; then
      echo "needs formatting: $f"
      failed=1
    fi
  done
  [[ $failed == 0 ]] && echo "all ${#FILES[@]} files formatted ($("$CF" --version))"
  exit $failed
fi

for f in "${FILES[@]}"; do "$CF" -i "$ROOT/$f"; done
echo "formatted ${#FILES[@]} files ($("$CF" --version))"
