# Container

A pattern for running in a container environment.

## Setup

`nagi serve` is designed to be stateless. Even if the container restarts, it reads the previous state from remote storage and resumes. For details, see [Serve Restart](../../architecture/serve/restart.md).

## Graceful Shutdown

When a stop signal is received, it stops issuing new tasks and waits for running Sync operations to complete. The wait timeout is configured via `terminationGracePeriodSeconds` in `nagi.yaml`. Set this value to match the shutdown grace period of your container environment.

## Storage Backend

Uses a remote backend (GCS / S3). Since local files are lost when the container restarts, cache, locks, and halt flags are stored remotely.

```yaml
# nagi.yaml
backend:
  type: gcs
  bucket: my-nagi-bucket
  prefix: my-project
```

For storage backend details, see [Storage](../../architecture/storage.md).

## Log Export

`logs.db` (execution history) uses SQLite and is stored locally. Since local `logs.db` is lost when the container restarts, configure [Export](../../architecture/export.md) to periodically export data to a data warehouse.
