# kind: Asset

宣言された期待状態に対して Nagi が evaluate を行い、Sync によって収束させるデータの単位です。reconciliation loop の中心となるリソースです。

```yaml
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
  labels:
    dbt/finance: ""
    dbt/daily: ""
spec:
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

`onDrift` の各エントリは [Conditions](./conditions.md) と [Sync](./sync.md) の名前をペアで指定します。エントリは上から順に評価され、最初に期待状態が Drifted のエントリの sync が実行されます（first-match）。すべてのエントリの期待状態が Ready であれば Asset は Ready です。

!!! tip
    `onDrift` が定義されていない Asset は常に Ready として扱われ、Sync は実行されません。

<!-- schema:auto-generated:start:AssetSpec -->

## Attributes

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `autoSync` | boolean | — | true | Controls automatic sync execution in `nagi serve`. Defaults to `true`. |
| `connection` | string | — | - | Name of the Connection resource for DB access. |
| `evaluateCacheTtl` | Duration | — | - | Default evaluate cache TTL for all conditions in this Asset. Conditions can override this with their own `evaluateCacheTtl`. |
| `onDrift` | list[OnDriftEntry] | — | [] | Condition-sync pairs evaluated in order. First entry whose conditions detect drift determines which sync to run. When omitted, the Asset is always Ready. |
| `upstreams` | list[string] | — | [] | Names of upstream Asset resources. |

<!-- schema:auto-generated:end:AssetSpec -->

<!-- schema:auto-generated:start:OnDriftEntry -->

### OnDriftEntry

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `conditions` | string | Yes | - | Name of the `kind: Conditions` resource whose conditions define drift. |
| `sync` | string | Yes | - | Name of the Sync resource to execute when drift is detected. |
| `mergePosition` | MergePosition | — | beforeOrigin | Controls insertion position during overlay merge. Not included in compiled output. |
| `with` | map[string, string] | — | {} | Template variables passed to the Sync and Conditions resources for argument interpolation. |

<!-- schema:auto-generated:end:OnDriftEntry -->
