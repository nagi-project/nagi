#!/bin/bash
# Block mise/cargo commands that run in the main repo root instead of a worktree.
# Allows commands that either:
#   - Start with "cd <worktree-path> &&"
#   - Are run when no worktrees exist (nothing to protect against)

input=$(cat)
command=$(echo "$input" | jq -r '.tool_input.command')

# Only check commands that include mise or cargo
if ! echo "$command" | grep -qE '(^|\s|&&\s*)(mise |cargo )'; then
  exit 0
fi

# If no worktrees exist besides main, no need to block
worktree_count=$(git worktree list 2>/dev/null | wc -l)
if [ "$worktree_count" -le 1 ]; then
  exit 0
fi

# Check if the command starts with cd to a worktree directory
if echo "$command" | grep -qE '^cd\s+.*/\.claude/worktrees/'; then
  exit 0
fi

# Block: mise/cargo without cd to worktree
jq -n '{
  hookSpecificOutput: {
    hookEventName: "PreToolUse",
    permissionDecision: "deny",
    permissionDecisionReason: "mise/cargo must run in a worktree, not in the main repo root. Prefix the command with: cd /path/to/.claude/worktrees/<name> &&"
  }
}'
