#!/usr/bin/env bash
# Check documentation rules in docs/.
# - All Markdown headings must be in English.

set -euo pipefail

errors=0

# --- Headings must be in English ---
# Detects Japanese characters (Hiragana, Katakana, CJK Unified Ideographs).
# Skips lines inside fenced code blocks (``` ... ```).

while IFS= read -r file; do
  in_code_block=false
  lineno=0
  while IFS= read -r line; do
    lineno=$((lineno + 1))
    # Toggle code block state on fence lines
    if [[ "$line" =~ ^\`\`\` ]]; then
      if $in_code_block; then
        in_code_block=false
      else
        in_code_block=true
      fi
      continue
    fi
    $in_code_block && continue
    # Check only Markdown headings
    if [[ "$line" =~ ^# ]]; then
      if printf '%s' "$line" | grep -qE '[ぁ-ん]|[ァ-ヶ]|[一-龠]|[Ａ-Ｚ]|[ａ-ｚ]'; then
        printf "  %s:%d: %s\n" "$file" "$lineno" "$line"
        errors=$((errors + 1))
      fi
    fi
  done < "$file"
done < <(find docs -name '*.md' -type f | sort)

if [ "$errors" -gt 0 ]; then
  printf "\nerror: %d heading(s) contain Japanese characters. Headings in docs/ must be in English.\n" "$errors"
  exit 1
fi
