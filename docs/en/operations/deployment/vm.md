# VM

A pattern for running as a persistent process on a VM or on-premises server.

## Setup

Manage `nagi serve` with a process manager. The process is automatically restarted if it exits abnormally.

- Linux: systemd, supervisord
- Windows: NSSM, Windows Service Wrapper

On restart, it resumes from the previous state. For details, see [Serve Restart](../../architecture/serve/restart.md).

## Graceful Shutdown

When a stop signal (SIGINT on Linux/macOS, Ctrl+C or CTRL_BREAK_EVENT on Windows) is received, it stops issuing new tasks and waits for running Sync operations to complete. The wait timeout is configured via `terminationGracePeriodSeconds` in `nagi.yaml`.

## Storage Backend

Uses the `local` backend.

```yaml
# nagi.yaml
backend:
  type: local
```
