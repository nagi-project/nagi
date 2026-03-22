# Quickstart

データウェアハウスへの接続なしで Nagi の一連の流れを体験できるサンプルプロジェクトです。

## Prerequisites

- Python 3.12 以上
- `pip install nagi-cli`

## Project Layout

!!! tip
    自分のプロジェクトで新規に始める場合は、[`nagi init`](./cli.md#init) を実行して `resources/` と `nagi.yaml` を生成してください。このサンプルでは作成済みの状態から始めます。

`examples/quickstart/` にサンプルプロジェクトが用意されています。

```bash
cd examples/quickstart
```

このプロジェクトには3つのリソースが定義されています。

**`resources/conditions.yaml`** — `greeting.txt` が存在するかをチェックする条件

```yaml
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: greeting-check
spec:
  - name: file-exists
    type: Command
    run: [test, -f, greeting.txt]
```

**`resources/sync.yaml`** — ファイルを作成する収束操作

```yaml
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: create-greeting
spec:
  run:
    type: Command
    args: [sh, -c, "echo 'Hello from Nagi!' > greeting.txt"]
```

**`resources/asset.yaml`** — 条件と Sync を対応付ける Asset

```yaml
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: greeting
spec:
  onDrift:
    - conditions: greeting-check
      sync: create-greeting
```

## Try It Out

### Step 1: Compile

```bash
nagi compile -y
```

`resources/` のリソースを解決し、`target/` に出力します。

### Step 2: Evaluate

```bash
nagi evaluate
```

`greeting.txt` がまだ存在しないため、Drifted と判定されます。

```json
[
  {
    "assetName": "greeting",
    "ready": false,
    "conditions": [
      {
        "conditionName": "file-exists",
        "conditionType": "Command",
        "status": { "state": "drifted", "reason": "'test' exited with code 1" }
      }
    ]
  }
]
```

### Step 3: Sync

```bash
nagi sync
```

プランが表示され、承認すると `greeting.txt` が作成されます。Sync 完了後に自動で re-evaluate が行われます。

### Step 4: Verify

```bash
cat greeting.txt
# Hello from Nagi!

nagi evaluate
```

`greeting.txt` が存在するため、Ready と判定されます。

```json
[
  {
    "assetName": "greeting",
    "ready": true,
    "conditions": [
      {
        "conditionName": "file-exists",
        "conditionType": "Command",
        "status": { "state": "ready" }
      }
    ]
  }
]
```

### Step 5: Serve

```bash
rm greeting.txt    # ファイルを削除して Drifted 状態にする
nagi serve
```

`nagi serve` を起動すると、Drifted を検知して自動で Sync を実行し、`greeting.txt` を再作成します。`Ctrl-C` で停止します。

## Cleanup

```bash
rm -rf target/ greeting.txt
```

## Next Steps

- [Concepts](./concepts.md) — Reconciliation Loop の仕組みを理解する
- [Resources](./configurations/resources/index.md) — リソースの種類と定義方法を知る
- [dbt Core](./integrations/dbt-core.md) — dbt プロジェクトと連携して試してみる
