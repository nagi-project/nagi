#!/usr/bin/env bash
# Verify that required gitignored files exist in git worktrees.
# Skips check when running in the main worktree (files already exist).

set -euo pipefail

# Only check in worktrees, not in the main repository
if ! git rev-parse --is-inside-work-tree &>/dev/null; then
  exit 0
fi
main_worktree="$(git worktree list --porcelain | head -1 | sed 's/^worktree //')"
current_dir="$(pwd -P)"
if [ "$current_dir" = "$main_worktree" ]; then
  exit 0
fi

errors=0

required_files=(
  "project_spec.md"
)

for f in "${required_files[@]}"; do
  if [ ! -f "$f" ]; then
    printf '{"continue":false,"stopReason":"missing: %s — copy from main worktree before starting work"}\n' "$f"
    exit 0
  fi
done
