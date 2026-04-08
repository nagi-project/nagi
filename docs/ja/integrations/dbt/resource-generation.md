# Resource Generation

[`nagi compile`](../../reference/cli.md#compile) は、dbt プロジェクトから Nagi のリソースを自動生成します。このページでは、生成されるリソースの内容とマージの動作を説明します。

以下のすべての例では、Origin 名を `my-project` とします。

## Mapping

Origin は dbt プロジェクトの `manifest.json` を読み取り、以下のマッピングで Nagi リソースを生成します。

| dbt resource | 生成されるリソース |
| --- | --- |
| dbt source | [kind: Asset](../../reference/resources/asset.md) |
| dbt model | [kind: Asset](../../reference/resources/asset.md) |
| dbt test | [kind: Conditions](../../reference/resources/conditions.md) |

生成される Asset の名前には Origin 名がプレフィックスとして付与されます: `{origin}.{model}`。詳細は [kind: Origin — Asset Naming](../../reference/resources/origin.md#asset-naming) を参照してください。

### dbt source

dbt source は Asset リソースに変換されます。

Origin の設定からは、`connection`, `autoSync` が付与されます。`upstreams` は設定されません。dbt source のタグは `dbt/` プレフィックス付きで `metadata.labels` に変換されます。

```yaml
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: my-project.raw-sales
spec:
  connection: my-bigquery
  autoSync: true
  onDrift:
    - conditions: dbt-tests-my-project.raw-sales
      sync: nagi-skip-sync
```

### dbt model

dbt model は Asset リソースに変換されます。

Origin の設定からは、`connection`, `autoSync` が付与されます。

dbt のリネージ（dbt model 間の依存関係、dbt source → dbt model の依存関係）は `spec.upstreams` に、dbt model のタグは `dbt/` プレフィックス付きで `metadata.labels` に変換されます。

```yaml
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: my-project.daily-sales
  labels:
    dbt/finance: ""
    dbt/daily: ""
spec:
  connection: my-bigquery
  autoSync: true
  upstreams:
    - my-project.raw-sales
  onDrift:
    - conditions: dbt-tests-my-project.daily-sales
      sync: my-project-dbt-run
```

### dbt test

dbt test は Conditions リソースに変換されます。

生成された Conditions は、その dbt test の対象である dbt model / dbt source の Asset に `onDrift` として設定されます。

```yaml
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: dbt-tests-my-project.daily-sales
spec:
  - name: dbt-test-daily-sales
    type: Command
    run: [dbt, test, --select, daily_sales]
```

## Drift Detection and Sync

期待状態からの変化を検知（Drifted）した際に、dbt resource によって Nagi が実行する Sync は異なります。

### Drift on dbt source

dbt source は外部システムから取り込まれたデータなので、Nagi や dbt では修復できません。

この場合、Sync は実行せず、この dbt source に依存する下流の Asset の処理をブロックします。外部でデータが修復されるまで待機します。

この動作には `nagi-skip-sync`（何もせず正常終了する組み込み Sync）が割り当てられます。

### Drift on dbt model

Nagi は `dbt run --select <model>` を実行してデータを再構築します。

Origin はこの動作を行う `{origin}-dbt-run`（例: `my-project-dbt-run`）という [Sync](../../reference/resources/sync.md) を自動生成します。Sync は `{{ asset.modelName }}` を使って Origin プレフィックスなしの元の dbt model 名を渡します。

```yaml
kind: Sync
metadata:
  name: my-project-dbt-run
spec:
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ asset.modelName }}", "--project-dir", "../dbt-project", "--profile", "my_project", "--target", "dev"]
```

Origin の `defaultSync` フィールドで、この Sync を別の Sync に変更できます。

## Merge with User-defined Resources

Origin が自動生成した Asset に対して、期待状態の定義を追加したい場合があります。`resources/` に同じ名前の `kind: Asset` を定義すると、両者の `onDrift` がリスト結合されます。マージが動作するのは `kind: Asset` のみです。

### Merge target

マージ対象は `onDrift` のみです。それ以外のフィールドは Origin の値が維持されます。

### Evaluation order

`onDrift` は上から順に評価され、最初に Drifted になったエントリの Sync が実行されます。

ユーザー定義 Asset の `onDrift` エントリに `mergePosition` を設定することで、マージ方法を指定できます。

- `beforeOrigin`: Origin エントリの前(デフォルト設定)
- `afterOrigin`: Origin エントリの後

### Name conflicts

Asset 以外のリソース（Connection, Conditions, Sync）が同名で重複した場合は compile エラーになります。

### Example

dbt model `daily_sales` に対して Freshness の期待状態を追加してデータの鮮度を監視する例を示します。`daily_sales` は、毎朝の dbt run で更新する日次集計テーブルとします。

#### 1. Origin-generated resources

Origin が自動生成した Conditions と Asset です。

```yaml
# target/conditions/dbt-tests-my-project.daily-sales.yaml（Origin 自動生成）
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: dbt-tests-my-project.daily-sales
spec:
  - name: dbt-test-daily-sales
    type: Command
    run: [dbt, test, --select, daily_sales]
```

```yaml
# target/assets/my-project.daily-sales.yaml（Origin 自動生成）
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: my-project.daily-sales
  labels:
    dbt/finance: ""
    dbt/daily: ""
spec:
  connection: my-bigquery
  upstreams:
    - my-project.raw-sales
  onDrift:
    - conditions: dbt-tests-my-project.daily-sales
      sync: my-project-dbt-run
```

#### 2. User-defined resources

毎朝更新されるテーブルなので、1日1回チェックし、25時間以上更新がなければ `dbt run` で再構築します（24時間 + ジョブ実行時間のバッファ 1時間）。

```yaml
# resources/freshness-daily-sales.yaml（ユーザー定義）
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: freshness-daily-sales
spec:
  - name: updated-within-25h
    type: Freshness
    maxAge: 25h
    interval: 24h
```

```yaml
# resources/daily-sales.yaml（ユーザー定義）
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: my-project.daily-sales
spec:
  onDrift:
    - conditions: freshness-daily-sales
      sync: my-project-dbt-run
      # mergePosition: afterOrigin
```

#### 3. Compiled result

##### `beforeOrigin`

デフォルト設定では、ユーザー定義のエントリが Origin エントリの前に配置されます。Freshness が先に評価されるため、データが25時間以上古ければ dbt test の結果にかかわらず `dbt run` が実行されます。

```yaml
# target/assets/my-project.daily-sales.yaml（compile 後）
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: my-project.daily-sales
  labels:
    dbt/finance: ""
    dbt/daily: ""
spec:
  connection: my-bigquery
  upstreams:
    - my-project.raw-sales
  onDrift:
    - conditions: freshness-daily-sales
      sync: my-project-dbt-run
    - conditions: dbt-tests-my-project.daily-sales
      sync: my-project-dbt-run
```

##### `afterOrigin`

`mergePosition: afterOrigin` を指定すると、ユーザー定義のエントリが Origin エントリの後に配置されます。dbt test が先に評価され、テストが通過した場合のみ Freshness が評価されます。

```yaml
# resources/freshness-daily-sales.yaml（afterOrigin を指定）
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: my-project.daily-sales
spec:
  onDrift:
    - conditions: freshness-daily-sales
      sync: my-project-dbt-run
      mergePosition: afterOrigin
```

```yaml
# target/assets/my-project.daily-sales.yaml（compile 後）
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: my-project.daily-sales
  labels:
    dbt/finance: ""
    dbt/daily: ""
spec:
  connection: my-bigquery
  upstreams:
    - my-project.raw-sales
  onDrift:
    - conditions: dbt-tests-my-project.daily-sales
      sync: my-project-dbt-run
    - conditions: freshness-daily-sales
      sync: my-project-dbt-run
```
