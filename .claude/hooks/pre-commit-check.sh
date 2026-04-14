#!/bin/bash

input=$(cat)
command=$(echo "$input" | jq -r '.tool_input.command')

if echo "$command" | grep -q 'git commit'; then
  # Block external repository references (owner/repo#123)
  ext_ref=$(echo "$command" | grep -oE '[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+#[0-9]+' | grep -v 'nagi-project/nagi' || true)
  if [ -n "$ext_ref" ]; then
    jq -n --arg refs "$ext_ref" '{
      hookSpecificOutput: {
        hookEventName: "PreToolUse",
        permissionDecision: "deny",
        permissionDecisionReason: ("Commit message contains external repository reference: " + $refs + ". Never reference external repos/issues/PRs in commit messages.")
      }
    }'
    exit 0
  fi

  # Block external GitHub URLs
  ext_url=$(echo "$command" | grep -oE 'github\.com/[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+' | grep -v 'github\.com/nagi-project/nagi' || true)
  if [ -n "$ext_url" ]; then
    jq -n --arg urls "$ext_url" '{
      hookSpecificOutput: {
        hookEventName: "PreToolUse",
        permissionDecision: "deny",
        permissionDecisionReason: ("Commit message contains external GitHub URL: " + $urls + ". Never reference external repos in commit messages.")
      }
    }'
    exit 0
  fi

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
