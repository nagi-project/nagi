# CLI

## Install

See [Get Started](../overview/get-started.md#install) for installation instructions.

## Output

Command output defaults to JSON. Use `--output text` for human-readable table output (`evaluate`, `status`, `ls`).

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
| `inspect` | Show the state change before and after Sync executions |
| `mcp` | Start the MCP server on stdio |

## Global options

| Option | Default | Description |
| --- | --- | --- |
| `--log-level` | `warn` | Set log level (`error`, `warn`, `info`, `debug`, `trace`). Overrides `NAGI_LOG_LEVEL` env var |
| `--project-dir` | `.` | Project directory |

## init

Prepares the environment so that `compile` can run.

```bash
nagi init [OPTIONS]
```

| Option | Description |
| --- | --- |
| `--overwrite-remote` | Overwrite existing remote configuration |

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

Lists all compiled resources.

```bash
nagi ls [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `--target-dir` | `target` | Compiled directory |
| `--output` | `json` | Output format (`json`, `text`) |
| `--no-pager` | — | Disable pager for terminal output |

## evaluate

Evaluates Assets against their desired state and determines Ready / Drifted.

```bash
nagi evaluate [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `--select` | — | Specify the Assets to evaluate |
| `--exclude` | — | Exclude assets matching this selector |
| `--target-dir` | `target` | Compiled directory |
| `--dry-run` | — | Show the desired state to be evaluated (does not execute queries or commands) |
| `--output` | `json` | Output format (`json`, `text`) |
| `--no-pager` | — | Disable pager for terminal output |

## sync

Executes Asset Sync operations. Displays a plan before execution and asks for user approval.

```bash
nagi sync [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `--select` | — | Specify the target Assets |
| `--exclude` | — | Exclude assets matching this selector |
| `--target-dir` | `target` | Compiled directory |
| `--stage` | — | Stages to execute (comma-separated: `pre`, `run`, `post`). When specified, skips evaluation after Sync completion |
| `--dry-run` | — | Show the commands to be executed (no side effects) |
| `--force` | — | Skip the dbt Cloud running-job check |
| `--auto-approve` | — | Skip interactive confirmation and execute all proposals |

## status

Shows cached evaluation results and recent Sync logs. Does not run Evaluate.

```bash
nagi status [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `--select` | — | Specify the target Assets |
| `--exclude` | — | Exclude assets matching this selector |
| `--target-dir` | `target` | Compiled directory |
| `--output` | `json` | Output format (`json`, `text`) |
| `--no-pager` | — | Disable pager for terminal output |

## serve

Starts compilation and a continuous reconciliation loop.

```bash
nagi serve [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `--select` | — | Specify the target Assets |
| `--exclude` | — | Exclude assets matching this selector |
| `--resources-dir` | `resources` | Resources directory |
| `--target-dir` | `target` | Compiled directory |

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

## inspect

Shows the state change before and after Sync executions for an Asset.

- Condition evaluation results (Ready / Drifted)
- Row count and object type (table, view, etc.)

```bash
nagi inspect <ASSET_NAME> [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `--limit` | `5` | Maximum number of Sync executions to show |
| `--changed-only` | — | Show only executions where state changed |
| `--target-dir` | `target` | Compiled directory |
| `--output` | `text` | Output format (`json`, `text`) |
| `--no-pager` | — | Disable pager for terminal output |

Inspection data is cached under `<stateDir>/inspections/<asset-name>/`.

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
| `label:key` | Select by label existence |
| `label:key=value` | Select by label key-value match |
| `+label:key` | Select by label, including upstream Assets |
| `label:key1,label:key2` | Intersection — Assets matching all criteria (AND) |

Multiple `--select` arguments are combined as union (OR). Comma-separated patterns within a single argument are intersected (AND).

```bash
# OR: Assets matching either selector
nagi evaluate --select daily-sales --select access-stats

# AND: Assets with both labels
nagi evaluate --select "label:dbt/finance,label:dbt/daily"

# Combined: (label:dbt/finance AND label:dbt/daily) OR access-stats
nagi evaluate --select "label:dbt/finance,label:dbt/daily" --select access-stats

# Select by label key-value pair
nagi evaluate --select "label:team=data-eng"
```

## --exclude syntax

`--exclude` uses the same selector syntax as `--select`. Assets matching any `--exclude` selector are removed from the result after `--select` is applied.

```bash
# Evaluate all assets except monthly-report
nagi evaluate --exclude monthly-report

# Evaluate finance-labeled assets, excluding daily-labeled ones
nagi evaluate --select "label:dbt/finance" --exclude "label:dbt/daily"
```
