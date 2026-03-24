# Serve Scenarios

`nagi serve` の動作を具体的なケースで説明します。各ケースでは、evaluate と sync がどのタイミングで実行されるかを時系列で示します。

なお、すべてのケースで `autoSync: true` を前提としています。

## Case 1: Linear Dependency Chain

```mermaid
graph LR
    A["A (interval なし)"] --> B["B (interval なし)"] --> C["C (interval なし)"]
```

sync は各 Asset で1回ずつ、上流から順に収束します。

```mermaid
sequenceDiagram
    participant A
    participant B
    participant C

    Note over A: evaluate → Drifted
    activate A
    Note over A: sync
    Note over A: re-evaluate → Ready
    deactivate A

    A->>B: upstream Ready

    Note over B: evaluate → Drifted
    activate B
    Note over B: sync
    Note over B: re-evaluate → Ready
    deactivate B

    B->>C: upstream Ready

    Note over C: evaluate → Drifted
    activate C
    Note over C: sync
    Note over C: re-evaluate → Ready
    deactivate C
```

## Case 2: Multiple Upstreams Become Ready in Quick Succession

```mermaid
graph LR
    A["A (interval あり)"] --> X["X (interval なし)"]
    B["B (interval あり)"] --> X
    C["C (interval あり)"] --> X
```

A, B, C が近いタイミングで Ready に遷移した例です。**X の sync は1回だけです。**

```mermaid
sequenceDiagram
    participant A
    participant B
    participant C
    participant X

    Note over A: Ready に遷移
    A->>X: upstream Ready
    Note over X: evaluate → Drifted
    activate X
    Note over X: sync 開始

    Note over B: Ready に遷移
    B->>X: upstream Ready
    Note over X: evaluate をキューに追加<br>(sync 中のため sync は再要求されない)

    Note over X: sync 完了
    Note over X: re-evaluate → Ready
    deactivate X

    Note over C: Ready に遷移
    C->>X: upstream Ready
    Note over X: evaluate → Ready<br>(sync 不要)
```

## Case 3: Upstreams Become Ready with Large Intervals

Case 2 と同じグラフで、上流の Ready 遷移が間隔を空けて起きるケースです。一度 Ready になった X は、X 自身の状態が変わらない限り Ready のままです。**X の sync は1回だけです。**

```mermaid
sequenceDiagram
    participant A
    participant B
    participant C
    participant X

    Note over A: Ready に遷移
    A->>X: upstream Ready
    Note over X: evaluate → Drifted
    activate X
    Note over X: sync
    Note over X: re-evaluate → Ready
    deactivate X

    Note over A,X: 時間経過

    Note over B: Ready に遷移
    B->>X: upstream Ready
    Note over X: evaluate → Ready<br>(sync 不要)

    Note over A,X: 時間経過

    Note over C: Ready に遷移
    C->>X: upstream Ready
    Note over X: evaluate → Ready<br>(sync 不要)
```

## Case 4: Fan-out

```mermaid
graph LR
    A["A (interval あり)"] --> B["B (interval なし)"]
    A --> C["C (interval なし)"]
    A --> D["D (interval なし)"]
```

A が Ready に遷移すると、B, C, D の evaluate が同時に起動されます。B, C, D は互いに依存関係がないため、sync は並列に実行されます。

```mermaid
sequenceDiagram
    participant A
    participant B
    participant C
    participant D

    Note over A: Ready に遷移

    par
        A->>B: upstream Ready
        Note over B: evaluate → Drifted
        activate B
        Note over B: sync
        Note over B: re-evaluate → Ready
        deactivate B
    and
        A->>C: upstream Ready
        Note over C: evaluate → Drifted
        activate C
        Note over C: sync
        Note over C: re-evaluate → Ready
        deactivate C
    and
        A->>D: upstream Ready
        Note over D: evaluate → Drifted
        activate D
        Note over D: sync
        Note over D: re-evaluate → Ready
        deactivate D
    end
```

## Case 5: Diamond Dependency

```mermaid
graph LR
    A["A (interval あり)"] --> B["B (interval なし)"] --> X["X (interval なし)"]
    A --> C["C (interval なし)"] --> X
```

Fan-out と Fan-in の組み合わせです。A の Ready が B と C に伝播し、B と C の Ready がそれぞれ X に伝播します。B と C の sync 完了タイミングが異なるため、X の evaluate は2回起動されますが、**X の sync は1回だけです。**

```mermaid
sequenceDiagram
    participant A
    participant B
    participant C
    participant X

    Note over A: Ready に遷移

    par
        A->>B: upstream Ready
        Note over B: evaluate → Drifted
        activate B
        Note over B: sync
        Note over B: re-evaluate → Ready
        deactivate B
    and
        A->>C: upstream Ready
        Note over C: evaluate → Drifted
        activate C
        Note over C: sync
    end

    B->>X: upstream Ready
    Note over X: evaluate → Drifted
    activate X
    Note over X: sync 開始

    Note over C: re-evaluate → Ready
    deactivate C
    C->>X: upstream Ready
    Note over X: evaluate をキューに追加<br>(sync 中のため sync は再要求されない)

    Note over X: sync 完了
    Note over X: re-evaluate → Ready
    deactivate X
```

B と C の sync 完了が近いタイミングであっても、X の sync が実行中であれば重複実行は発生しません。

## Case 6: Interval with Upstream Propagation

```mermaid
graph LR
    A["A (interval あり)"] --> B["B (interval あり)"]
```

B はポーリングと上流の状態変化の両方で evaluate が起動されます。

```mermaid
sequenceDiagram
    participant A
    participant B
    participant Timer as B の interval

    Timer->>B: interval 経過
    Note over B: evaluate → Ready

    Timer->>B: interval 経過
    Note over B: evaluate → Ready

    Note over A: Ready に遷移
    A->>B: upstream Ready
    Note over B: evaluate → Drifted
    activate B
    Note over B: sync
    Note over B: re-evaluate → Ready
    deactivate B

    Timer->>B: interval 経過
    Note over B: evaluate → Ready
```

interval による evaluate は上流の状態変化とは独立して動作します。どちらが先に Drifted を検出しても、sync は同じように実行されます。
