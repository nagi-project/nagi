# Resource Configurations

Nagi の動作に必要な定義は、すべてリソースとして `resources/` に配置します。

すべてのリソースは Kubernetes と同じ構造を持ちます。リソース種別は kind として指定します。

```yaml
apiVersion: nagi.io/v1alpha1
kind: <リソース種別>
metadata:
  name: <一意な名前>
  labels:
    team: data-eng
  annotations:
    description: "..."
    owner: "data-team@example.com"
spec:
  ...
```

<!-- schema:auto-generated:start:Metadata -->

## Attributes

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `name` | string | Yes | - | Unique name of the resource. |
| `annotations` | map[string, string] | — | - | Non-identifying metadata for descriptions, owner contacts, or other arbitrary information. |
| `labels` | map[string, string] | — | - | Key-value pairs for filtering with `--select label:key` or `--select label:key=value`. |

<!-- schema:auto-generated:end:Metadata -->

### labels

labels は `--select` セレクターで Asset の絞り込みに使用します。

```yaml
metadata:
  name: daily-sales
  labels:
    team: data-eng
    env: prod
```

```bash
# team ラベルが存在する Asset を選択
nagi evaluate --select "label:team"

# team=data-eng の Asset を選択
nagi evaluate --select "label:team=data-eng"
```

Origin (dbt) から自動生成された Asset では、dbt のタグが `dbt/` プレフィックス付きで labels に変換されます。

```yaml
metadata:
  name: my-project.daily-sales
  labels:
    dbt/finance: ""
    dbt/daily: ""
```

### annotations

annotations はリソースの補足情報を格納します。Nagi はこの値を処理に使用しません。

```yaml
metadata:
  name: daily-sales
  annotations:
    description: "日次売上の集計指標"
    owner: "data-team@example.com"
```

## kind

kind ごとに reconciliation loop の中での役割が異なります。

| kind | reconciliation loop での役割 |
| --- | --- |
| [Asset](./asset.md) | evaluate と sync の対象。`onDrift` で期待状態とその収束操作のペアを定義する。`upstreams` で上流 Asset を参照する。`connection` は、期待状態の評価で外部データソースへ問い合わせる必要がある場合に指定する |
| [Connection](./connection.md) | 外部データソースへの接続情報。`Freshness` や `SQL` など、データを問い合わせる Conditions で使用する |
| [Sync](./sync.md) | sync の手順定義。pre → run → post の3ステージ |
| [Conditions](./conditions.md) | 期待状態の定義をまとめたリソース。複数の Asset で共有できる |
| [Origin](./origin.md) | 他のソフトウェアが持つデータの構成情報から Asset を自動生成する |
| [Identity](./identity.md) | 認証スコープを宣言する。Connection、Sync、Command 条件から参照し、操作ごとに使用するクレデンシャルを制御する |

## Template Variables

Sync と Conditions の `args` 内で以下のテンプレート変数を使用できます。compile 時に展開されます。

| Variable | Description |
| --- | --- |
| `{{ asset.name }}` | Asset の `metadata.name`。Origin 生成の Asset では Origin 名がプレフィックスとして付与される（例: `my-project.orders`） |
| `{{ asset.modelName }}` | Origin プレフィックスなしの元のモデル名（例: `orders`）。ユーザー定義の Asset では `{{ asset.name }}` と同じ値。`dbt run --select` など外部ツールへの引数に使用する |
| `{{ sync.<key> }}` | Asset の `onDrift[].with` で指定した `<key>` の値。例えば `{{ sync.selector }}` は `with.selector` の値に展開される |

### `{{ asset.name }}` and `{{ asset.modelName }}`

これらの変数は常に利用可能で、`with` の指定は不要です。

```yaml
kind: Sync
metadata:
  name: my-sync
spec:
  run:
    type: Command
    args: ["echo", "{{ asset.name }}"]
    # Origin 名 my-project, model 名 orders の場合 "my-project.orders" に展開
```

### `{{ sync.<key> }}`

Asset の `onDrift[].with` でキーと値のペアを Sync に渡します。各キーが `{{ sync.<key> }}` 変数になります。

```yaml
# Sync 定義: {{ sync.selector }} をプレースホルダーとして使用
kind: Sync
metadata:
  name: dbt-default
spec:
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ sync.selector }}"]
```

```yaml
# Asset 側で with を使って値を渡す
onDrift:
  - conditions: daily-sla
    sync: dbt-default
    with:
      selector: "+daily_sales"   # {{ sync.selector }} が "+daily_sales" に展開される
```

`with` を省略した場合、`{{ asset.name }}` と `{{ asset.modelName }}` は展開されますが、`{{ sync.<key> }}` は展開されずそのまま残ります。
