# kind: Sync

収束操作の定義です。pre → run → post の3ステージで構成され、外部コマンドをサブプロセスとして呼び出します。pre と post は省略できます。

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

<!-- schema:auto-generated:start:SyncSpec -->

## Attributes

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `run.args` | list[string] | Yes | - | Command and arguments in argv format. |
| `run.type` | StepType | Yes | - | Execution type for this step (currently only `Command`). |
| `run.env` | map[string, string] | — | {} | Environment variables to set for the subprocess. |
| `post` | SyncStep | — | - | Optional step executed after the main sync command. |
| `pre` | SyncStep | — | - | Optional step executed before the main sync command. |

<!-- schema:auto-generated:end:SyncSpec -->

<!-- schema:auto-generated:start:SyncStep -->

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `args` | list[string] | Yes | - | Command and arguments in argv format. |
| `type` | StepType | Yes | - | Execution type for this step (currently only `Command`). |
| `env` | map[string, string] | — | {} | Environment variables to set for the subprocess. |

<!-- schema:auto-generated:end:SyncStep -->
