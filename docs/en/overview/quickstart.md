# Quickstart

A sample project that lets you experience Nagi's workflow without connecting to a data warehouse.

## Prerequisites

- Python 3.10 or later
- `pip install nagi-cli`

## Project Layout

!!! tip
    To start fresh in your own project, run [`nagi init`](../reference/cli.md#init) to generate `resources/` and `nagi.yaml`. This sample starts from a pre-created state.

A sample project is provided in `examples/quickstart/`.

```bash
cd examples/quickstart
```

This project defines two Assets. `farewell` depends on `greeting` as its upstream.

```text
greeting → farewell
```

`resources/conditions.yaml` — Defines the desired state as the file existing

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

`resources/sync.yaml` — Convergence operation that creates the file

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

`resources/asset.yaml` — Asset that maps desired state to its convergence operation

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

Validates and compiles resources in `resources/` and outputs them to `target/`.

### Step 2: Evaluate

```bash
nagi evaluate
```

Since the files do not exist yet, both Assets evaluate to Drifted.

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

Nagi displays a plan. After you approve, it creates `greeting.txt` and `farewell.txt`. After Sync completes, Nagi automatically runs Evaluate.

### Step 4: Verify

```bash
cat greeting.txt
# Hello from Nagi!

cat farewell.txt
# Goodbye from Nagi!

nagi evaluate
```

Both Assets evaluate to Ready.

### Step 5: Serve

```bash
nagi serve
```

Running `nagi serve` periodically evaluates the desired state and automatically executes Sync when drift is detected.

#### Upstream Changes Propagate Downstream

In another terminal, delete `greeting.txt`.

```bash
rm greeting.txt
```

The following sequence is automatically executed:

1. `greeting` detects Drifted → executes `create-greeting` sync → recreates `greeting.txt`
2. `greeting` transitions to Ready → `farewell` sync is automatically triggered because its upstream returned to Ready → recreates `farewell.txt`

When an upstream Asset returns to Ready, downstream Assets skip Evaluate and directly execute Sync.

#### Only Downstream Becomes Drifted

Delete only `farewell.txt`.

```bash
rm farewell.txt
```

`greeting` remains Ready with no change. `farewell` detects Drifted at the next scheduled evaluation and executes `create-farewell` sync to recreate `farewell.txt`.

Stop with `Ctrl-C`.

## Cleanup

```bash
rm -rf target/ greeting.txt farewell.txt
```

## Next Steps

- [Concepts](./concepts.md) — Understand the concepts
- [Get Started](./get-started.md) — Set up your environment
- [Architecture](../architecture/index.md) — Learn about the architecture in detail
- [Resources](../reference/resources/index.md) — Learn about resource types and how to define them
