# Resource Configurations

Nagi の動作に必要な定義は、すべてリソースとして `resources/` に配置します。

すべてのリソースは Kubernetes と同じ構造を持ちます。リソース種別は kind として指定します。

```yaml
apiVersion: nagi.io/v1alpha1
kind: <リソース種別>
metadata:
  name: <一意な名前>
spec:
  ...
```

kind ごとに reconciliation loop の中での役割が異なります。

| kind | reconciliation loop での役割 |
| --- | --- |
| [Asset](./asset.md) | evaluate と sync の対象。`onDrift` で条件と収束操作のペアを定義する。`upstreams` で上流 Asset を参照し、`connection` で DB 接続を指定する |
| [Connection](./connection.md) | Asset が参照するデータウェアハウスへの接続情報。evaluate 時のクエリ実行に使用する |
| [Sync](./sync.md) | sync の手順定義。pre → run → post の3ステージ |
| [Conditions](./conditions.md) | 再利用可能な条件のセット。複数の Asset で共有できる |
| [Origin](./origin.md) | 外部プロジェクトから Asset を自動生成する |

## Template Variables

Sync と Conditions の `args` 内で以下のテンプレート変数を使用できます。Asset の `onDrift[].with` で指定した値が compile 時に展開されます。

| Variable | Description |
| --- | --- |
| `{{ asset.name }}` | この Sync / Conditions を参照する Asset の名前に展開される |
| `{{ sync.<key> }}` | Asset の `onDrift[].with` で指定した値に展開される |

```yaml
# Sync 定義
kind: Sync
metadata:
  name: dbt-default
spec:
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ sync.selector }}"]
```

```yaml
# Asset 側で with を指定
onDrift:
  - conditions: daily-sla
    sync: dbt-default
    with:
      selector: "+daily_sales"   # {{ sync.selector }} が "+daily_sales" に展開される
```

`with` を省略した場合、`{{ asset.name }}` は自動で展開されますが、`{{ sync.<key> }}` は展開されずそのまま残ります。
