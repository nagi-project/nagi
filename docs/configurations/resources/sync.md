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

## Template variables

`args` 内で以下のテンプレート変数を使用できます。

| Variable | Description |
| --- | --- |
| `{{ asset.name }}` | この Sync を参照する Asset の名前に展開される |
| `{{ sync.<key> }}` | Asset の `onDrift[].with` で指定した値に展開される |

```yaml
# Sync 定義
spec:
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ sync.selector }}"]

# Asset 側で with を指定
onDrift:
  - conditions: daily-sla
    sync: dbt-default
    with:
      selector: "+daily_sales"   # {{ sync.selector }} が "+daily_sales" に展開される
```

`with` を省略した場合、`{{ asset.name }}` は自動で展開されますが、`{{ sync.<key> }}` は展開されずそのまま残ります。

<!-- schema:auto-generated:start:SyncSpec -->

## Attributes

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `run` | any | Yes | - | The main convergence step. |
| `post` | SyncStep | — | - | Optional step executed after the main sync command. |
| `pre` | SyncStep | — | - | Optional step executed before the main sync command. |

<!-- schema:auto-generated:end:SyncSpec -->
