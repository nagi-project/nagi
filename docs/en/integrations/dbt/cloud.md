# dbt Cloud

This page describes how to use Nagi with a dbt Cloud environment.

## Recommended Approach

When using Nagi alongside dbt Cloud, we recommend starting with state evaluation and notifications. Setting `autoSync: false` on the [Origin](../../reference/resources/origin.md) applies this to all automatically generated Assets.

```yaml
kind: Origin
spec:
  type: DBT
  connection: my-bigquery
  projectDir: ../dbt-project
  autoSync: false
```

dbt Cloud jobs update models on their existing schedules, and Nagi periodically inspects the data state and sends notifications when drift is detected. For a gradual path toward automated convergence, see [Concepts — From Monitoring to Automation](../../overview/concepts.md#from-monitoring-to-automation).

## Prerequisites

[dbt CLI](https://docs.getdbt.com/docs/core/installation-overview) (dbt-core >= 1.0) must be installed in the environment where Nagi runs.

## Init

Running [`nagi init`](../../reference/cli.md#init) generates Connection and Origin in `resources/`, the same as [dbt Core](./core.md#init).

When using dbt Cloud, add the `dbtCloud` field to the generated Connection.

```yaml
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: my-bigquery
spec:
  dbtProfile:
    profile: my_project
    target: dev
  dbtCloud:
    credentialsFile: ~/.dbt/dbt_cloud.yml
```

`credentialsFile` is optional and defaults to `~/.dbt/dbt_cloud.yml`.

This credentials file can be downloaded from the [dbt Cloud UI](https://docs.getdbt.com/docs/cloud/configure-cloud-cli#configure-the-dbt-cli) (project settings CLI section -> "Download CLI configuration file"). Save it to `~/.dbt/dbt_cloud.yml`.

## Compile

Same as [dbt Core](./core.md): runs `dbt compile` against the local dbt project to generate `manifest.json`. For resource generation details, see [Resource Generation](./resource-generation.md).

## Evaluate

Same as [dbt Core](./core.md#evaluate).

## Sync

When executing Sync, dbt CLI is run as a subprocess, the same as dbt Core.

In a dbt Cloud environment, Nagi's Sync and dbt Cloud jobs may update the same model simultaneously. To prevent this, Sync Control checks for running dbt Cloud jobs before executing a Sync.

## Sync Control

When the Asset's Connection has `dbtCloud` configured, Nagi checks whether any dbt Cloud jobs are running before executing a Sync. At compile time, it parses the `execute_steps` of each dbt Cloud job to identify jobs that involve the target Asset. At Sync execution time, if that job is running, the Sync is aborted.

1. At compile time, all jobs are fetched from the Jobs API, model names are extracted from `--select` in `execute_steps`, and related jobs are identified for each Asset
2. At Sync execution time, running jobs are fetched from the Runs API, and it checks whether any job related to the target Asset is running
  - If no related job is running, the Sync is executed
  - If a related job is running, an error is returned

### API

Nagi uses the following dbt Cloud Administrative API endpoints.

| Timing | API endpoint | Purpose |
| --- | --- | --- |
| compile | `GET /api/v2/accounts/{account_id}/jobs/` | Fetch `execute_steps` of all jobs to build the mapping between Assets and jobs |
| Sync | `GET /api/v2/accounts/{account_id}/runs/?status=3` | Fetch running jobs (status=3: Running) to check whether any job related to the target Asset is running |

### Required Permissions

The dbt Cloud API token requires the following permissions.

| Permission | Reason |
| --- | --- |
| Read access to Jobs | Fetching job definitions and `execute_steps` |
| Read access to Runs | Checking for running jobs |

The `token-value` included in `~/.dbt/dbt_cloud.yml` is used for API authentication. The token is read from the file at the time of the API call but is not retained in memory.

## Customization

Same as [dbt Core](./core.md#customization).
