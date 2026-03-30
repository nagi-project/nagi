# Resource Configurations

Place all resource definitions that Nagi needs in `resources/`.

All resources share the same structure as Kubernetes. The resource type is specified in the `kind` field.

```yaml
apiVersion: nagi.io/v1alpha1
kind: <resource type>
metadata:
  name: <unique name>
spec:
  ...
```

Each kind has a different role within the reconciliation loop.

| kind | Role in the reconciliation loop |
| --- | --- |
| [Asset](./asset.md) | The target of Evaluate and Sync. `onDrift` defines pairs of desired state and the corresponding convergence operation. `upstreams` references upstream Assets, and `connection` specifies the DB connection |
| [Connection](./connection.md) | Connection information for the data warehouse referenced by Assets. Used for query execution during Evaluate |
| [Sync](./sync.md) | Sync procedure definition. Three stages: pre, run, post |
| [Conditions](./conditions.md) | A resource that groups desired state definitions. Can be shared across multiple Assets |
| [Origin](./origin.md) | Auto-generates Assets from data structure information managed by other software |

## Template Variables

The following template variables can be used within `args` of Sync and Conditions. Values specified in the Asset's `onDrift[].with` are expanded at compile time.

| Variable | Description |
| --- | --- |
| `{{ asset.name }}` | Expands to the name of the Asset that references this Sync / Conditions |
| `{{ sync.<key> }}` | Expands to the value specified in the Asset's `onDrift[].with` |

```yaml
# Sync definition
kind: Sync
metadata:
  name: dbt-default
spec:
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ sync.selector }}"]
```

```yaml
# Specifying with on the Asset side
onDrift:
  - conditions: daily-sla
    sync: dbt-default
    with:
      selector: "+daily_sales"   # {{ sync.selector }} expands to "+daily_sales"
```

When `with` is omitted, `{{ asset.name }}` is automatically expanded, but `{{ sync.<key> }}` remains unexpanded.
