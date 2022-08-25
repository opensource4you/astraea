/throttles
===

取得與套用限流相關的設定

- [取得叢集的所有 throttle 設定](#取得叢集的所有-throttle-設定)

## 取得叢集的所有 throttle 設定

```shell
GET /throttles
```

cURL 範例

```shell
curl -X GET http://localhost:8001/throttles
```

JSON Response 範例

- `brokers`: 所有上線的 Kafka 節點的限流量設定
  - `1001`: 敘述這個 broker ID 的 kafka 節點設定
    - `ingress`: 特定 Kafka 節點的 replication 流入流量的限制(單位 bytes/sec)，附註只有 `topics` 中有記錄到的對象，
      其 replication 流量會受限制，如果此欄位不存在則代表此節點的 replication 流入流量沒有被限制。
    - `egress`: 特定 Kafka 節點的 replication 流出流量的限制(單位 bytes/sec)，附註只有 `topics` 中有記錄到的對象，
      其 replication 流量會受限制，如果此欄位不存在則代表此節點的 replication 流出流量沒有被限制。
- `topics`: 當前叢集中，被 replication 限流影響的 logs
  - `name`: 套用 replication 限流的 topic
  - `partition`: 套用 replication 限流的 partition
  - `broker`: 套用 replication 限流的 broker ID
  - `type`: 被套用限流的 log 身份，可以是 `leader` 或 `follower`，如果沒有標記這個欄位，意味著二者皆是

```json
{
    "brokers": {
        "1001": { "ingress": 1000, "egress": 1000 },
        "1002": { "ingress": 1000, "egress": 1000 },
        "1003": { "ingress": 1000 },
        "1004": {}
    },
    "topics": [
      { "name": "MyTopicB", "partition": 0, "broker": 0 },
      { "name": "MyTopicC", "partition": 1, "broker": 1, "type": "leader" },
      { "name": "MyTopicD", "partition": 2, "broker": 2, "type": "follower" }
    ]
}
```