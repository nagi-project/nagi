# Quickstart

データウェアハウスへの接続なしで Nagi の一連の流れを体験できるサンプルプロジェクトです。

## Prerequisites

- Python 3.10 以上
- `pip install nagi-cli`

## Project Layout

!!! tip
    自分のプロジェクトで新規に始める場合は、[`nagi init`](../reference/cli.md#init) を実行して `resources/` と `nagi.yaml` を生成してください。このサンプルでは作成済みの状態から始めます。

`examples/quickstart/` にサンプルプロジェクトが用意されています。

```bash
cd examples/quickstart
```

このプロジェクトには2つの Asset が定義されています。`farewell` は `greeting` を上流として依存しています。

```text
greeting → farewell
```

`resources/conditions.yaml` — ファイルが存在していることを期待状態として定義

```yaml
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: greeting-check
spec:
  - name: file-exists
    type: Command
    run: [test, -f, greeting.txt]
---
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: farewell-check
spec:
  - name: file-exists
    type: Command
    run: [test, -f, farewell.txt]
```

`resources/sync.yaml` — ファイルを作成する収束操作

```yaml
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: create-greeting
spec:
  run:
    type: Command
    args: [sh, -c, "echo 'Hello from Nagi!' > greeting.txt"]
---
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: create-farewell
spec:
  run:
    type: Command
    args: [sh, -c, "echo 'Goodbye from Nagi!' > farewell.txt"]
```

`resources/asset.yaml` — 期待状態とその収束操作を対応付ける Asset

```yaml
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: greeting
spec:
  onDrift:
    - conditions: greeting-check
      sync: create-greeting
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: farewell
spec:
  upstreams:
    - greeting
  onDrift:
    - conditions: farewell-check
      sync: create-farewell
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

ファイルがまだ存在しないため、両方の Asset が Drifted と判定されます。

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
  },
  {
    "assetName": "farewell",
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

プランが表示され、承認すると `greeting.txt` と `farewell.txt` が作成されます。Sync 完了後に自動で evaluate が行われます。

### Step 4: Verify

```bash
cat greeting.txt
# Hello from Nagi!

cat farewell.txt
# Goodbye from Nagi!

nagi evaluate
```

両方の Asset が Ready と判定されます。

### Step 5: Serve

```bash
nagi serve
```

`nagi serve` を起動すると、期待状態を定期的に評価し、Drifted を検知したら自動で Sync を実行します。

#### Upstream Changes Propagate Downstream

別のターミナルで `greeting.txt` を削除します。

```bash
rm greeting.txt
```

以下の流れが自動で実行されます。

1. `greeting` が Drifted を検知 → `create-greeting` sync を実行 → `greeting.txt` を再作成
2. `greeting` が Ready に遷移 → 上流伝播により `farewell` の sync が自動起動 → `farewell.txt` を再作成

上流 Asset が Ready に戻ると、下流 Asset は evaluate をスキップして直接 sync を実行します。

#### Only Downstream Becomes Drifted

`farewell.txt` のみを削除します。

```bash
rm farewell.txt
```

`greeting` は Ready のまま変化しません。`farewell` が次の定期評価で Drifted を検知し、`create-farewell` sync を実行して `farewell.txt` を再作成します。

`Ctrl-C` で停止します。

## Cleanup

```bash
rm -rf target/ greeting.txt farewell.txt
```

## Next Steps

- [Concepts](./concepts.md) — コンセプトを理解する
- [Get Started](./get-started.md) — セットアップを行う
- [Architecture](../architecture/index.md) — アーキテクチャの詳細を知る
- [Resources](../reference/resources/index.md) — リソースの種類と定義方法を知る
