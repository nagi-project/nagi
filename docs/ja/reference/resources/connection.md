# kind: Connection

データウェアハウスへの接続情報です。Asset から参照され、Evaluate 時のクエリ実行で使用します。

## Supported Data Warehouses

| spec.type | データウェアハウス | 接続方法 | 認証方式 |
| --- | --- | --- | --- |
| dbt | BigQuery<br>DuckDB<br>Snowflake | dbt adapter | profiles.yml |
| bigquery | BigQuery | BigQuery REST API | Application Default Credentials<br>Service Account Key |
| duckdb | DuckDB | DuckDB CLI | なし |
| snowflake | Snowflake | Snowflake SQL REST API | Key-Pair JWT |

<!-- schema:auto-generated:start:ConnectionSpec -->

## Attributes

### type: dbt

Connection resolved via dbt profiles.yml.

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `profile` | string | Yes | - | Profile name as defined in `~/.dbt/profiles.yml`. |
| `dbtCloud.credentialsFile` | string | — | - | Path to the dbt Cloud credentials file. Defaults to `~/.dbt/dbt_cloud.yml`. |
| `identity` | string | — | - | Reference to a `kind: Identity` resource for authentication scope. |
| `profilesDir` | string | — | - | Directory containing profiles.yml. If omitted, uses `~/.dbt/`. |
| `target` | string | — | - | If omitted, the default target in profiles.yml is used. |

### type: BigQuery

BigQuery REST API connection.

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `dataset` | string | Yes | - | BigQuery dataset name. |
| `project` | string | Yes | - | GCP project ID that contains the dataset. |
| `executionProject` | string | — | - | GCP project ID used for query execution billing. Defaults to `project` if omitted. |
| `identity` | string | — | - | Reference to a `kind: Identity` resource for authentication scope. |
| `keyfile` | string | — | - | Path to the service account JSON key file. Required when `method` is `service-account`. |
| `method` | string | — | - | Authentication method. `oauth` (Application Default Credentials) or `service-account`. Defaults to `oauth`. |
| `timeout` | Duration | — | - | Query timeout (e.g. "30s", "1h"). Falls back to `NagiConfig::default_timeout` when omitted. |

### type: DuckDB

DuckDB connection via the `duckdb` CLI.

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `path` | string | Yes | - | Path to the DuckDB database file. |
| `identity` | string | — | - | Reference to a `kind: Identity` resource for authentication scope. |

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
| `identity` | string | — | - | Reference to a `kind: Identity` resource for authentication scope. |
| `role` | string | — | - | Role to use for the session. Uses the user's default role if omitted. |
| `timeout` | Duration | — | - | Query timeout (e.g. "30s", "1h"). Falls back to `NagiConfig::default_timeout` when omitted. |

<!-- schema:auto-generated:end:ConnectionSpec -->
