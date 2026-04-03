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

The following template variables can be used within `args` of Sync and Conditions. They are expanded at compile time.

| Variable | Description |
| --- | --- |
| `{{ asset.name }}` | The Asset's `metadata.name`. For Origin-generated Assets this is prefixed with the Origin name (e.g. `my-project.orders`) |
| `{{ asset.modelName }}` | The original model name without the Origin prefix (e.g. `orders`). For user-defined Assets, same as `{{ asset.name }}`. Use this for external tool arguments such as `dbt run --select` |
| `{{ sync.<key> }}` | The value of `<key>` from the Asset's `onDrift[].with` map. For example, `{{ sync.selector }}` expands to the value of `with.selector` |

### `{{ asset.name }}` and `{{ asset.modelName }}`

These variables are always available and do not require `with`.

```yaml
kind: Sync
metadata:
  name: my-sync
spec:
  run:
    type: Command
    args: ["echo", "{{ asset.name }}"]
    # For Origin "my-project", model "orders": expands to "my-project.orders"
```

### `{{ sync.<key> }}`

The Asset's `onDrift[].with` passes key-value pairs to the Sync. Each key becomes a `{{ sync.<key> }}` variable.

```yaml
# Sync definition: uses {{ sync.selector }} as a placeholder
kind: Sync
metadata:
  name: dbt-default
spec:
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ sync.selector }}"]
```

```yaml
# Asset passes the value via with
onDrift:
  - conditions: daily-sla
    sync: dbt-default
    with:
      selector: "+daily_sales"   # {{ sync.selector }} expands to "+daily_sales"
```

When `with` is omitted, `{{ asset.name }}` and `{{ asset.modelName }}` are still expanded, but `{{ sync.<key> }}` remains unexpanded.
