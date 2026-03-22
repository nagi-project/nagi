# kind: Source

Asset が依存する上流データの宣言です。[kind: Connection](./connection.md) への参照を持ちます。

また、[kind: Asset](./asset.md) の `spec.sources` から `ref` で参照されます。

```yaml
apiVersion: nagi.io/v1alpha1
kind: Source
metadata:
  name: raw-sales
spec:
  connection: my-bigquery
```

<!-- schema:auto-generated:start:SourceSpec -->

## Attributes

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `connection` | string | Yes | - | Name of the Connection resource this Source uses. |

<!-- schema:auto-generated:end:SourceSpec -->
