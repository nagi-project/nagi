#!/bin/bash

input=$(cat)
command=$(echo "$input" | jq -r '.tool_input.command')

if echo "$command" | grep -q 'git commit'; then
  if ! mise run check >/dev/null 2>&1; then
    jq -n '{
      hookSpecificOutput: {
        hookEventName: "PreToolUse",
        permissionDecision: "deny",
        permissionDecisionReason: "mise run check failed. Run `mise run check` to see details."
      }
    }'
    exit 0
  fi
fi
