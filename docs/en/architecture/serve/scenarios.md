# Serve Scenarios

This page explains `nagi serve` behavior through specific scenarios. Each scenario shows the timeline of when Evaluate and Sync are executed.

All scenarios assume `autoSync: true`.

## Scenario 1: Linear Dependency Chain

```mermaid
graph LR
    A["A (no interval)"] --> B["B (no interval)"] --> C["C (no interval)"]
```

A has 2 evaluations (initial + re-evaluate) and 1 Sync. B and C each have 1 evaluation (re-evaluate only) and 1 Sync. Convergence proceeds from upstream to downstream.

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

    A->>B: upstream Ready → directly trigger sync
    activate B
    Note over B: sync
    Note over B: re-evaluate → Ready
    deactivate B

    B->>C: upstream Ready → directly trigger sync
    activate C
    Note over C: sync
    Note over C: re-evaluate → Ready
    deactivate C
```

## Scenario 2: Multiple Upstreams Become Ready in Quick Succession

```mermaid
graph LR
    A["A (has interval)"] --> X["X (no interval)"]
    B["B (has interval)"] --> X
    C["C (has interval)"] --> X
```

An example where A, B, and C transition to Ready in close succession. X has 2 Syncs and 2 evaluations (re-evaluate after Sync only). B's propagation is ignored because X is mid-Sync. C's propagation is accepted after X's Sync completes, and a second Sync is executed. This execution is necessary to reflect C's data changes into X.

```mermaid
sequenceDiagram
    participant A
    participant B
    participant C
    participant X

    Note over A: Transition to Ready
    A->>X: upstream Ready → directly trigger sync
    activate X
    Note over X: sync start

    Note over B: Transition to Ready
    B->>X: upstream Ready
    Note over X: (ignored, mid-sync)

    Note over X: sync complete
    Note over X: re-evaluate → Ready
    deactivate X

    Note over C: Transition to Ready
    C->>X: upstream Ready → directly trigger sync
    activate X
    Note over X: sync
    Note over X: re-evaluate → Ready
    deactivate X
```

## Scenario 3: Upstreams Become Ready with Large Intervals

The same graph as Scenario 2, but with upstream Ready transitions occurring at well-spaced intervals. X has 3 Syncs and 3 evaluations (re-evaluate after Sync only). Each is a legitimate execution to reflect each upstream's data changes into X.

```mermaid
sequenceDiagram
    participant A
    participant B
    participant C
    participant X

    Note over A: Transition to Ready
    A->>X: upstream Ready → directly trigger sync
    activate X
    Note over X: sync
    Note over X: re-evaluate → Ready
    deactivate X

    Note over A,X: Time passes

    Note over B: Transition to Ready
    B->>X: upstream Ready → directly trigger sync
    activate X
    Note over X: sync
    Note over X: re-evaluate → Ready
    deactivate X

    Note over A,X: Time passes

    Note over C: Transition to Ready
    C->>X: upstream Ready → directly trigger sync
    activate X
    Note over X: sync
    Note over X: re-evaluate → Ready
    deactivate X
```

## Scenario 4: Fan-out

```mermaid
graph LR
    A["A (has interval)"] --> B["B (no interval)"]
    A --> C["C (no interval)"]
    A --> D["D (no interval)"]
```

When A transitions to Ready, Sync for B, C, and D is directly triggered. Each Asset has 1 Sync and 1 evaluation (re-evaluate only). Since B, C, and D have no dependencies on each other, their Syncs run in parallel.

```mermaid
sequenceDiagram
    participant A
    participant B
    participant C
    participant D

    Note over A: Transition to Ready

    par
        A->>B: upstream Ready → directly trigger sync
        activate B
        Note over B: sync
        Note over B: re-evaluate → Ready
        deactivate B
    and
        A->>C: upstream Ready → directly trigger sync
        activate C
        Note over C: sync
        Note over C: re-evaluate → Ready
        deactivate C
    and
        A->>D: upstream Ready → directly trigger sync
        activate D
        Note over D: sync
        Note over D: re-evaluate → Ready
        deactivate D
    end
```

## Scenario 5: Diamond Dependency

```mermaid
graph LR
    A["A (has interval)"] --> B["B (no interval)"] --> X["X (no interval)"]
    A --> C["C (no interval)"] --> X
```

A combination of fan-out and fan-in. When A transitions to Ready, Sync for B and C is directly triggered. When B and C transition to Ready, they each directly trigger X's Sync. X has 1 Sync and 1 evaluation (re-evaluate only). Even if C becomes Ready while X is mid-Sync, Sync is not re-requested.

```mermaid
sequenceDiagram
    participant A
    participant B
    participant C
    participant X

    Note over A: Transition to Ready

    par
        A->>B: upstream Ready → directly trigger sync
        activate B
        Note over B: sync
        Note over B: re-evaluate → Ready
        deactivate B
    and
        A->>C: upstream Ready → directly trigger sync
        activate C
        Note over C: sync
    end

    B->>X: upstream Ready → directly trigger sync
    activate X
    Note over X: sync start

    Note over C: re-evaluate → Ready
    deactivate C
    C->>X: upstream Ready
    Note over X: (ignored, mid-sync)

    Note over X: sync complete
    Note over X: re-evaluate → Ready
    deactivate X
```

Even if Syncs for B and C complete at nearly the same time, duplicate execution does not occur as long as X's Sync is already running.

## Scenario 6: Interval with Upstream Propagation

```mermaid
graph LR
    A["A (has interval)"] --> B["B (has interval)"]
```

B operates via both polling-based Evaluate and direct Sync from upstream state changes. In this example, B has 1 Sync (directly triggered by upstream Ready) and 4 evaluations (3 from interval + 1 re-evaluate after Sync).

```mermaid
sequenceDiagram
    participant A
    participant B
    participant Timer as B's interval timer

    Timer->>B: interval elapsed
    Note over B: evaluate → Ready

    Timer->>B: interval elapsed
    Note over B: evaluate → Ready

    Note over A: Transition to Ready
    A->>B: upstream Ready → directly trigger sync
    activate B
    Note over B: sync
    Note over B: re-evaluate → Ready
    deactivate B

    Timer->>B: interval elapsed
    Note over B: evaluate → Ready
```

Interval-based evaluate operates independently of upstream state changes. While upstream Drifted-to-Ready transitions trigger Sync directly (skipping Evaluate), periodic Evaluate via interval continues to run.

## Scenario 7: Upstream Drifted Blocks Downstream Operations

```mermaid
graph LR
    A["A (has interval)"] --> B["B (has interval)"] --> C["C (no interval)"]
```

While upstream A is Drifted, downstream B and C wait for all operations. B has an interval, but Evaluate is not run because the upstream is Drifted. Once A's Sync completes and it becomes Ready, upstream Ready is sent downstream.

```mermaid
sequenceDiagram
    participant A
    participant B
    participant Timer as B's interval timer
    participant C

    Note over A: evaluate → Drifted
    activate A

    Timer->>B: interval elapsed
    Note over B: Waiting (A is Drifted)

    Timer->>B: interval elapsed
    Note over B: Waiting (A is Drifted)

    Note over A: sync
    Note over A: re-evaluate → Ready
    deactivate A

    A->>B: upstream Ready → directly trigger sync
    activate B
    Note over B: sync
    Note over B: re-evaluate → Ready
    deactivate B

    B->>C: upstream Ready → directly trigger sync
    activate C
    Note over C: sync
    Note over C: re-evaluate → Ready
    deactivate C
```
