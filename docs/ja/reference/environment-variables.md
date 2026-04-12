# Environment Variables

Nagi は Sync のステージ（pre / run / post）、Conditions の `type: Command`、Origin の `dbt compile` の 3 つの場面で外部コマンドをサブプロセスとして実行します。このページでは、サブプロセスに渡る環境変数の仕組みを説明します。

## What Reaches the Subprocess

サブプロセスに渡る環境変数は、次の 2 つの経路で構成されます。

- Nagi が親プロセスから引き継ぐ、OS 動作に必要な最小セット。プラットフォームごとに固定
- YAML リソースの `env` フィールドに宣言した値

これら以外の環境変数は、Nagi を起動したシェルに設定されていても、サブプロセスには渡りません。

### OS Essentials

Nagi は OS の基本動作に必要な以下の環境変数のみを親プロセスから引き継ぎます。

#### Unix

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

#### Windows

| Variable | Purpose |
| --- | --- |
| `SystemRoot` | Windows システムディレクトリ |
| `SystemDrive` | システムドライブレター |
| `ComSpec` | コマンドプロセッサのパス |
| `PATH` | 実行ファイルの探索 |
| `PATHEXT` | 実行ファイル拡張子 |
| `USERPROFILE` | ユーザープロファイルディレクトリ |
| `HOMEDRIVE` | ホームドライブレター |
| `HOMEPATH` | ホームディレクトリパス |
| `APPDATA` | アプリケーションデータ |
| `LOCALAPPDATA` | ローカルアプリケーションデータ |
| `TEMP` | 一時ファイルディレクトリ |
| `TMP` | 一時ファイルディレクトリ |
| `ProgramData` | 共有アプリケーションデータ |
| `ProgramFiles` | プログラムインストールディレクトリ |
| `ProgramFiles(x86)` | 32 ビットプログラムインストールディレクトリ |
| `ProgramW6432` | 64 ビットプログラムインストールディレクトリ |
| `CommonProgramFiles` | 共有プログラムコンポーネント |
| `CommonProgramFiles(x86)` | 32 ビット共有プログラムコンポーネント |
| `CommonProgramW6432` | 64 ビット共有プログラムコンポーネント |
| `ALLUSERSPROFILE` | 全ユーザープロファイルディレクトリ |
| `COMPUTERNAME` | マシン名 |
| `LOGONSERVER` | ログオンサーバー |
| `PUBLIC` | パブリックプロファイルディレクトリ |
| `USERDOMAIN` | ユーザードメイン |
| `USERDOMAIN_ROAMINGPROFILE` | ローミングプロファイルのユーザードメイン |
| `NUMBER_OF_PROCESSORS` | プロセッサ数 |
| `PROCESSOR_ARCHITECTURE` | プロセッサアーキテクチャ |

このリストは固定で、設定で変更することはできません。追加の環境変数をサブプロセスに渡したい場合は `env` フィールドに宣言してください。

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
