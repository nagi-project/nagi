# Nagi

[![PyPI](https://img.shields.io/pypi/v/nagi-cli)](https://pypi.org/project/nagi-cli/) [![CI](https://github.com/nagi-project/nagi/actions/workflows/ci.yml/badge.svg)](https://github.com/nagi-project/nagi/actions/workflows/ci.yml) [![License](https://img.shields.io/pypi/l/nagi-cli)](https://github.com/nagi-project/nagi/blob/main/LICENSE) [![Python](https://img.shields.io/pypi/pyversions/nagi-cli)](https://pypi.org/project/nagi-cli/)

Nagi keeps data in its desired state.

## Motivation

State evaluation, routine ELT, and data incident response are often carried out as separate activities — different tools, different runbooks, different moments. When a scheduled job succeeds but the data is stale, the gap between "the pipeline ran" and "the data is correct" surfaces as an incident that lives outside the pipeline itself.

These activities are points on the same continuum: observe state, decide it needs correction, and correct it. Nagi places them in a single reconciliation loop so you can move between monitoring, manual recovery, and automated convergence without changing tools or vocabulary.

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

- Declarative — Define the desired state; let Nagi handle convergence.
- Composable — Use with your existing tools, or let Nagi take the wheel.
- AI-collaborative — Designed for humans and AI agents to work as one.

## Install

```bash
pip install nagi-cli
```

See the [Quickstart](https://nagi-project.dev/overview/quickstart/) for a full walkthrough, or browse the [Documentation](https://nagi-project.dev).

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
