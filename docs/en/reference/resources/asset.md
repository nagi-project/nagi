# kind: Asset

A unit of data. Corresponds to a table or view in a data warehouse. The central resource that is the target of Evaluate and Sync in the reconciliation loop.

```yaml
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  tags: [finance, daily]
  connection: my-bigquery
  upstreams:
    - raw-sales
  onDrift:
    - conditions: daily-sla
      sync: dbt-default
      with:
        selector: "+daily_sales"
    - conditions: sales-quality
      sync: sales-full-reload
  autoSync: true
```

Each entry in `onDrift` specifies a pair of [Conditions](./conditions.md) and [Sync](./sync.md) names. Entries are evaluated from top to bottom, and the sync of the first entry that evaluates to Drifted is executed (first-match). If all entries evaluate to Ready, the Asset is Ready.

!!! tip
    An Asset with no `onDrift` defined is always treated as Ready, and no Sync is executed.

<!-- schema:auto-generated:start:AssetSpec -->

## Attributes

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `autoSync` | boolean | — | true | Controls automatic sync execution in `nagi serve`. Defaults to `true`. |
| `connection` | string | — | - | Name of the Connection resource for DB access. |
| `evaluateCacheTtl` | Duration | — | - | Default evaluate cache TTL for all conditions in this Asset. Conditions can override this with their own `evaluateCacheTtl`. |
| `onDrift` | list[OnDriftEntry] | — | [] | Condition-sync pairs evaluated in order. First entry whose conditions detect drift determines which sync to run. When omitted, the Asset is always Ready. |
| `tags` | list[string] | — | [] | Tags for filtering with `--select tag:X`. |
| `upstreams` | list[string] | — | [] | Names of upstream Asset resources. |

<!-- schema:auto-generated:end:AssetSpec -->

<!-- schema:auto-generated:start:OnDriftEntry -->

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `conditions` | string | Yes | - | Name of the `kind: Conditions` resource whose conditions define drift. |
| `sync` | string | Yes | - | Name of the Sync resource to execute when drift is detected. |
| `mergePosition` | MergePosition | — | beforeOrigin | Controls insertion position during overlay merge. Not included in compiled output. |
| `with` | map[string, string] | — | {} | Template variables passed to the Sync and Conditions resources for argument interpolation. |

<!-- schema:auto-generated:end:OnDriftEntry -->
