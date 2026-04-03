# CLI

## Install

See [Get Started](../overview/get-started.md#install) for installation instructions.

## Output

All command output is in JSON format.

## Subcommands

| Subcommand | Description |
| --- | --- |
| `init` | Prepare the environment so that `compile` can run |
| `compile` | Compile Asset definitions and output to `target/` |
| `ls` | List compiled resources |
| `evaluate` | Evaluate Assets against their desired state |
| `status` | Show cached evaluation results and recent Sync logs |
| `sync` | Execute Asset Sync |
| `serve` | Compile Assets and start the reconciliation loop |
| `serve resume` | Resume suspended Assets |
| `serve halt` | Halt all Assets at once |
| `export` | Export execution logs to a data warehouse |
| `mcp` | Start the MCP server on stdio |

## init

Prepares the environment so that `compile` can run.

```bash
nagi init
```

Interactively configures the Origin, generates a Connection, and verifies the connection. You select an Origin type and proceed with type-specific settings. Idempotent and safe to re-run.

## compile

Compiles resource definitions from `resources/` and outputs them to `target/`. If an existing `target/` is present, prompts for overwrite confirmation.

```bash
nagi compile [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `--resources-dir` | `resources` | Input directory |
| `--target-dir` | `target` | Output directory |
| `-y, --yes` | — | Skip overwrite confirmation |

## ls

Lists all compiled resources as JSON.

```bash
nagi ls [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `--target-dir` | `target` | Compiled directory |

## evaluate

Evaluates Assets against their desired state and determines Ready / Drifted.

```bash
nagi evaluate [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `--select` | — | Specify the Assets to evaluate |
| `--target-dir` | `target` | Compiled directory |
| `--cache-dir` | — | Cache directory |
| `--dry-run` | — | Show the desired state to be evaluated (does not execute queries or commands) |

## sync

Executes Asset Sync operations. Displays a plan before execution and asks for user approval.

```bash
nagi sync [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `--select` | — | Specify the target Assets |
| `--target-dir` | `target` | Compiled directory |
| `--stage` | — | Stages to execute (comma-separated: `pre`, `run`, `post`). When specified, skips evaluation after Sync completion |
| `--cache-dir` | — | Cache directory |
| `--dry-run` | — | Show the commands to be executed (no side effects) |
| `--force` | — | Skip the dbt Cloud running-job check |

## status

Shows cached evaluation results and recent Sync logs. Does not run Evaluate.

```bash
nagi status [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `--select` | — | Specify the target Assets |
| `--target-dir` | `target` | Compiled directory |
| `--cache-dir` | — | Cache directory |

## serve

Starts compilation and a continuous reconciliation loop.

```bash
nagi serve [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `--select` | — | Specify the target Assets |
| `--resources-dir` | `resources` | Resources directory |
| `--target-dir` | `target` | Compiled directory |
| `--cache-dir` | — | Cache directory |
| `--project-dir` | `.` | Project directory |

### serve resume

Resumes suspended Assets.

```bash
nagi serve resume [OPTIONS]
```

| Option | Description |
| --- | --- |
| `--select` | Specify the Assets to resume (interactive selection when omitted) |

### serve halt

Halts all Assets at once.

```bash
nagi serve halt [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `--reason` | `manual halt` | Halt reason. Included in the suspended file and notification message |

## export

Exports execution logs (`logs.db`) to a data warehouse. Requires the `export` setting in [`nagi.yaml`](./project.md).

```bash
nagi export [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `--select` | — | Specify the table names to export (`evaluate_logs`, `sync_logs`, `sync_evaluations`) |
| `--dry-run` | — | Show the number of unexported rows (does not transfer) |

## mcp

Starts the MCP server on stdio.

```bash
nagi mcp [OPTIONS]
```

| Option | Description |
| --- | --- |
| `--allow-sync` | Also expose the Sync tool |

By default, only read-only tools (`nagi_status`, `nagi_evaluate`) are exposed.

## --select syntax

`--select` supports a selector syntax.

| Syntax | Description |
| --- | --- |
| `name` | The specified Asset (e.g. `my-project.orders`) |
| `+name` | The specified Asset and all upstream Assets |
| `name+` | The specified Asset and all downstream Assets |
| `+name+` | The specified Asset and all upstream and downstream Assets |
| `N+name` | The specified Asset and N levels of upstream Assets |
| `name+N` | The specified Asset and N levels of downstream Assets |
| `tag:finance` | Select by tag |
| `+tag:finance` | Select by tag, including upstream Assets |
