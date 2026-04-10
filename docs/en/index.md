# Nagi

Nagi is a workflow engine that declaratively defines the desired state of data and continuously performs evaluation and convergence.

## Motivation

A successful job does not guarantee that data is as expected. Even when a job completes normally, data can be stale, contain NULLs, or have inconsistent aggregations.

Nagi starts by evaluating whether data is as expected. It continuously evaluates the desired state of data and, when drift is detected, runs the convergence operation defined for it. With desired states and convergence operations declared up front, Nagi unifies state evaluation, routine Extract/Load/Transform, and incident response into a single loop.

### Traditional Approach

```mermaid
graph LR
    subgraph Orchestrator
        Schedule["Schedule /<br/>Event trigger"] --> Run["Run job"]
        Run --> JobFail{"Job failed?"}
        JobFail -->|No| Test["Data test"]
        Test --> DataFail{"Data corrupted?"}
        DataFail -->|No| Schedule
        JobFail -->|Yes| Alert["Alert"]
        DataFail -->|Yes| Alert
    end
    subgraph Human["User"]
        Investigate["Investigate & fix"]
    end
    Alert --> Investigate
    Investigate --> Orchestrator
```

### Nagi Approach

```mermaid
graph LR
    subgraph Nagi
        Eval["Evaluate data state"]
        Drift{"Diverged from<br>desired state?"}
        Conv["Run convergence<br>operation"]
    end
    subgraph User
        Define["Define desired state<br>of data"]
    end
    subgraph UserAI["User / AI Agent"]
        Check["Check state"]
    end
    Define --> Eval
    Eval --> Drift
    Drift -->|No| Eval
    Drift -->|Yes| Conv
    Conv --> Eval
    Nagi -->|"Alert"| UserAI
    UserAI -->|"Manual operation"| Nagi
```

## Principles

- Declarative — Define the desired state; let the engine converge.
- Composable — Use with your existing tools, or let Nagi take the wheel.
- AI-collaborative — Designed for humans and AI agents to work as one.

## What's Next

- [Concepts](./overview/concepts.md) — Learn how the Reconciliation Loop works
