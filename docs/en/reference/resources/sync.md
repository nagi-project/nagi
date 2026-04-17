# kind: Sync

The definition of a convergence operation. Composed of three stages: pre, run, post. Invokes external commands as subprocesses. pre and post can be omitted.

```yaml
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: dbt-default
spec:
  pre:
    type: Command
    args: ["python", "pre.py"]
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ sync.selector }}"]
  post:
    type: Command
    args: ["python", "post.py"]
```

## Environment Variables

Each step (pre, run, post) can declare environment variables via the `env` field. Only these declared values and a minimal set of OS essentials are passed to the subprocess — the parent shell's environment is not inherited. Values can reference the Nagi process's own environment using `${VAR}` syntax. See [Environment Variables](../environment-variables.md) for details.

<!-- schema:auto-generated:start:SyncSpec -->

## Attributes

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `run.args` | list[string] | Yes | - | Command and arguments in argv format. |
| `run.type` | StepType | Yes | - | Execution type for this step (currently only `Command`). |
| `run.env` | map[string, string] | — | {} | Environment variables to set for the subprocess. |
| `run.identity` | string | — | - | Reference to a `kind: Identity` resource. Overrides the Sync-level identity for this stage. |
| `run.timeout` | Duration | — | - | Per-step timeout. Overrides the Sync-level `timeout` for this stage. |
| `identity` | string | — | - | Reference to a `kind: Identity` resource. Applied to all stages unless overridden per-stage. |
| `post` | SyncStep | — | - | Optional step executed after the main sync command. |
| `pre` | SyncStep | — | - | Optional step executed before the main sync command. |
| `timeout` | Duration | — | - | Sync-wide timeout applied to each step unless overridden. Falls back to `NagiConfig::default_timeout` when omitted. |

<!-- schema:auto-generated:end:SyncSpec -->

<!-- schema:auto-generated:start:SyncStep -->

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `args` | list[string] | Yes | - | Command and arguments in argv format. |
| `type` | StepType | Yes | - | Execution type for this step (currently only `Command`). |
| `env` | map[string, string] | — | {} | Environment variables to set for the subprocess. |
| `identity` | string | — | - | Reference to a `kind: Identity` resource. Overrides the Sync-level identity for this stage. |
| `timeout` | Duration | — | - | Per-step timeout. Overrides the Sync-level `timeout` for this stage. |

<!-- schema:auto-generated:end:SyncStep -->
