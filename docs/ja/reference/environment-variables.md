# Environment Variables

Nagi は Sync のステージ（pre / run / post）、Conditions の `type: Command`、Origin の `dbt compile` の 3 つの場面で外部コマンドをサブプロセスとして実行します。このページでは、サブプロセスに渡る環境変数の仕組みを説明します。

## What Reaches the Subprocess

サブプロセスの環境変数は、プラットフォームごとのベースとユーザー宣言値から構成されます。親シェルの環境変数はそのまま引き継がれません。

### Unix

Nagi は親プロセスの環境変数をクリアし、以下の固定セットのみを通します。

| Variable | Purpose |
| --- | --- |
| `PATH` | 実行ファイルの探索 |
| `HOME` | ホームディレクトリ |
| `USER` | 現在のユーザー名 |
| `LOGNAME` | ログイン名 |
| `LANG` | ロケール設定 |
| `LC_ALL` | ロケール上書き |
| `LC_CTYPE` | 文字分類のロケール |
| `TZ` | タイムゾーン |
| `TMPDIR` | 一時ファイルディレクトリ |

このリストは固定で、設定で変更できません。それ以外の親プロセスの環境変数は除外されます。

### Windows

Nagi は Windows の `CreateEnvironmentBlock` API を使ってベース環境を構築します。ユーザープロファイルの完全な環境変数セットが得られ、PowerShell や .NET ランタイムの起動に必要なシステム変数やログオン時に合成される変数を含みます。

親シェルのセッションにのみ存在する変数は含まれません。`set` や `$env:` で設定したシェル固有の変数は、`env` に宣言しない限りサブプロセスには渡りません。

ユーザープロファイルにクラウド認証情報を永続的なユーザー環境変数として登録している場合、それらはベースに含まれます。除外するには、認証情報を永続的なユーザー環境変数として登録しないでください。`env` と `${VAR}` を使って Nagi プロセスから明示的に渡す方法を推奨します。

## Declaring Environment Variables

Sync のステップ、Conditions の `type: Command`、Origin `type: DBT` の `env` フィールドに環境変数を宣言します。

```yaml
kind: Sync
metadata:
  name: dbt-run
spec:
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ asset.modelName }}"]
    env:
      GOOGLE_APPLICATION_CREDENTIALS: ${GOOGLE_APPLICATION_CREDENTIALS}
      DBT_PROFILES_DIR: /custom/profiles
```

## Referencing Parent Process Variables

Nagi を起動したシェルの環境変数をサブプロセスに渡したい場合は、`env` の値に `${VAR}` 形式で参照を書きます。

```yaml
env:
  GOOGLE_APPLICATION_CREDENTIALS: ${GOOGLE_APPLICATION_CREDENTIALS}
  CUSTOM_PATH: ${HOME}/custom
  LITERAL_VALUE: my-fixed-value
```

### Syntax

- `${VAR}` 形式のみサポート
- `VAR` は `[A-Za-z_][A-Za-z0-9_]*` に一致する必要があります
- 大文字小文字を区別します。`${foo}` と `${FOO}` は別の変数です
- リテラル文字列と `${VAR}` 参照は 1 つの値の中で混在できます

### Resolution

- `${VAR}` の解決は YAML パース時ではなく、サブプロセス起動の直前に行われます
- 参照先の変数が Nagi プロセスに設定されていない場合、サブプロセスの起動はエラーで失敗します

## Where `env` Can Be Used

| リソース | フィールド | サブプロセス |
| --- | --- | --- |
| Sync | `spec.pre.env`<br>`spec.run.env`<br>`spec.post.env` | Sync ステージのコマンド |
| Conditions (`type: Command`) | `spec[].env` | `type: Command` のコマンド |
| Origin (`type: DBT`) | `spec.env` | `dbt compile` |

## Nagi-injected Variables

Nagi は Sync サブプロセスに以下の変数を自動で注入します。ユーザー宣言の `env` よりも後に設定されるため、上書きはできません。

| 変数 | 説明 |
| --- | --- |
| `NAGI_EXECUTION_ID` | Sync 実行を一意に識別する UUID。データウェアハウス上のジョブと Sync 実行を紐付けるために使用します（ジョブラベルやクエリタグなどとして）。設定例は [dbt Core 連携](../integrations/dbt/core.md#propagating-execution_id-to-bigquery-jobs) を参照してください。 |
| `TRACEPARENT` | execution ID から導出された [W3C Trace Context](https://www.w3.org/TR/trace-context/) ヘッダー。OpenTelemetry 対応ツールが自動的に親トレースとして認識します。 |
