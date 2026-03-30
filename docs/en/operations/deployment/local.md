# Local

A pattern for development and testing.

## Setup

```bash
nagi serve
```

`nagi serve` automatically runs compile on startup. If you modify resource definitions or `nagi.yaml`, stop the process with `Ctrl-C` and run it again.

## Storage Backend

Uses the default `local` backend. State data is stored in `nagiDir` (default: `~/.nagi`).

```yaml
# nagi.yaml
backend:
  type: local
```
