# kind: Connection

Connection information for a data warehouse. Referenced by Assets and used for query execution during Evaluate.

## Supported Data Warehouses

| spec.type | Data Warehouse | Connection Method | Authentication |
| --- | --- | --- | --- |
| dbt | BigQuery<br>DuckDB<br>Snowflake | dbt adapter | profiles.yml |
| bigquery | BigQuery | BigQuery REST API | Application Default Credentials<br>Service Account Key |
| duckdb | DuckDB | DuckDB CLI | None |
| snowflake | Snowflake | Snowflake SQL REST API | Key-Pair JWT |

<!-- schema:auto-generated:start:ConnectionSpec -->

## Attributes

### type: dbt

Connection resolved via dbt profiles.yml.

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `profile` | string | Yes | - | Profile name as defined in `~/.dbt/profiles.yml`. |
| `dbtCloud` | DbtCloudSpec | — | - | Optional dbt Cloud configuration for running-job checks before sync. |
| `target` | string | — | - | If omitted, the default target in profiles.yml is used. |

### type: BigQuery

BigQuery REST API connection.

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `dataset` | string | Yes | - | BigQuery dataset name. |
| `project` | string | Yes | - | GCP project ID that contains the dataset. |
| `executionProject` | string | — | - | GCP project ID used for query execution billing. Defaults to `project` if omitted. |
| `keyfile` | string | — | - | Path to the service account JSON key file. Required when `method` is `service-account`. |
| `method` | string | — | - | Authentication method. `oauth` (Application Default Credentials) or `service-account`. Defaults to `oauth`. |
| `timeoutSeconds` | integer | — | - | Query timeout in seconds. |

### type: DuckDB

DuckDB connection via the `duckdb` CLI.

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `path` | string | Yes | - | Path to the DuckDB database file. |

### type: Snowflake

Snowflake SQL REST API connection with Key-Pair JWT authentication.

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `account` | string | Yes | - | Snowflake account identifier (e.g. `myorg-myaccount`). |
| `database` | string | Yes | - | Database name. |
| `privateKeyPath` | string | Yes | - | Path to the RSA private key file (PKCS#8 PEM format) for JWT authentication. |
| `schema` | string | Yes | - | Schema name. |
| `user` | string | Yes | - | Snowflake login user name. |
| `warehouse` | string | Yes | - | Warehouse name. |
| `role` | string | — | - | Role to use for the session. Uses the user's default role if omitted. |

<!-- schema:auto-generated:end:ConnectionSpec -->
