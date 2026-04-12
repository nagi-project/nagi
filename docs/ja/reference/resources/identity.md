# kind: Identity

認証スコープを宣言するリソースです。Connection、Sync、Command 条件から名前で参照し、操作ごとに使用するクレデンシャルを切り替えます。

```yaml
apiVersion: nagi.io/v1alpha1
kind: Identity
metadata:
  name: bq-evaluator
spec:
  type: env
  env:
    GOOGLE_APPLICATION_CREDENTIALS: ${NAGI_EVAL_KEYFILE}
```

## How It Works

Connection、Sync ステージ、Command 条件が Identity を名前で参照すると、Identity の `spec.env` に定義された環境変数が対象スコープに注入されます。

Connection の場合、env マップはクエリ実行時の認証情報解決に使われます。サブプロセス（Sync ステージと Command 条件）の場合、env マップは OS 基本セットとステップレベルの `env` 宣言の間に注入されます。レイヤー構成の詳細は[環境変数](../environment-variables.md)を参照してください。

値は `${VAR}` 形式で Nagi プロセスの環境変数を参照できます。参照はパース時ではなく使用時に解決されるため、展開後のクレデンシャル値が構造体フィールドに保持されることはありません。

## Referencing an Identity

### From Connection

```yaml
kind: Connection
metadata:
  name: my-bigquery
spec:
  type: bigquery
  project: my-project
  dataset: analytics
  identity: bq-evaluator
```

BigQuery Connection に `identity` フィールドを指定すると、Identity の env が認証に使われます。Identity の env に `GOOGLE_APPLICATION_CREDENTIALS` がある場合、その値が ADC ファイルパスとして使用されます。

DuckDB と Snowflake の Connection は Identity をまだサポートしていません。`identity` を指定した場合、compile 時に warning が出力されます。

### From Sync

```yaml
kind: Sync
metadata:
  name: dbt-run
spec:
  identity: bq-syncer
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ asset.name }}"]
```

Sync レベルの `identity` は、ステージごとに上書きされない限り、すべてのステージ（pre, run, post）に適用されます。

### From a Sync Stage

```yaml
kind: Sync
metadata:
  name: dbt-run
spec:
  identity: default-id
  pre:
    type: Command
    args: ["python", "prepare.py"]
    identity: pre-id
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ asset.name }}"]
```

`pre` は `pre-id` を使用します。`run` と `post` は Sync レベルの `default-id` を継承します。

### From Command Condition

```yaml
kind: Conditions
metadata:
  name: check-data
spec:
  - name: validate
    type: Command
    run: ["python", "validate.py"]
    identity: validator-id
```

## Identity Resolution Order

各 Sync ステージで適用される Identity は次の順で決定します。

1. ステージ（pre, run, post）に `identity` が指定されていればそれを使う
2. 指定されていなければ Sync レベルの `identity` を継承する
3. どちらも指定されていなければ Identity は適用しない

## type: env

環境変数のキーと値のペアを宣言します。

| Attribute | Type | Required | Description |
| --- | --- | --- | --- |
| `env` | map[string, string] | Yes | 環境変数名と値のマップ。値は `${VAR}` 形式で Nagi プロセスの環境変数を参照可能 |
