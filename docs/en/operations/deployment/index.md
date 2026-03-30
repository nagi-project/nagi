# Deployment

## Server

`nagi serve` runs as a single process. The following table summarizes deployment patterns for different execution environments.

| Pattern | Use case | Storage backend |
| --- | --- | --- |
| [Local](./local.md) | Development and testing | local |
| [VM](./vm.md) | Running as a persistent process | local |
| [Container](./container.md) | Container environments such as Cloud Run | remote (GCS / S3) |

For architecture details, see [Serve Internals](../../architecture/serve/internals.md). For storage backend details, see [Storage](../../architecture/storage.md).

## Client

When using a remote storage backend, you can use the CLI as a client from an environment separate from the server.

Configure the same remote storage backend as the server in the client environment's `nagi.yaml`.

- [`nagi status`](../../reference/cli.md#status) — Check the state of each Asset
- [`nagi evaluate`](../../reference/cli.md#evaluate) / [`nagi sync`](../../reference/cli.md#sync) — Manual execution
- [`nagi serve resume`](../../reference/cli.md#serve-resume) / [`nagi serve halt`](../../reference/cli.md#serve-halt) — Control serve
