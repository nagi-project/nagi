# Resource Configurations

Nagi の動作に必要な定義は、すべてリソースとして `resources/` に配置します。

すべてのリソースは Kubernetes と同じ構造を持ちます。リソース種別は kind として指定します。

```yaml
apiVersion: nagi.io/v1alpha1
kind: <リソース種別>
metadata:
  name: <一意な名前>
spec:
  ...
```

kind ごとに reconciliation loop の中での役割が異なります。

| kind | reconciliation loop での役割 |
| --- | --- |
| Asset | evaluate と sync の対象。`onDrift` で条件と収束操作のペアを定義する。`upstreams` で上流 Asset を参照し、`connection` で DB 接続を指定する |
| Connection | Asset が参照するデータウェアハウスへの接続情報。evaluate 時のクエリ実行に使用する |
| Sync | sync の手順定義。pre → run → post の3ステージ |
| Conditions | 再利用可能な条件のセット。複数の Asset で共有できる |
| Origin | 外部プロジェクトから Asset を自動生成する |
