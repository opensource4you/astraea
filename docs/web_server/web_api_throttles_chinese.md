/throttles
===

取得與套用限流相關的設定

- [取得叢集的所有 throttle 設定](#取得叢集的所有-throttle-設定)
- [新增/覆蓋 throttle 設定](#新增/覆蓋-throttle-設定)
- [刪除 throttle 設定](#刪除-throttle-設定)
  - [移除整個 Topic 的 replication throttle](#移除整個-Topic-的-replication-throttle)
  - [移除整個 Topic/Partition 的 replication throttle](#移除整個-Topic/Partition-的-replication-throttle)
  - [移除整個 Topic/Partition/Replica 的 replication throttle](#移除整個-Topic/Partition/Replica-的-replication-throttle)
  - [移除特定 Topic/Partition/Replica 針對 leader 或是 follower 的 replication throttle](#移除特定-Topic/Partition/Replica-針對-leader-或是-follower-的-replication-throttle)
  - [移除特定節點的 replication 輸入限流量](#移除特定節點的-replication-輸入限流)
  - [移除特定節點的 replication 輸出限流量](#移除特定節點的-replication-輸出限流)
  - [移除特定節點的 replication 輸出和輸入限流量](#移除特定節點的-replication-輸出和輸入限流)


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
  - `id`: Kafka 節點編號
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
    "brokers": [
      { "id": 1001, "ingress": 1000, "egress": 1000 },
      { "id": 1002, "ingress": 1000, "egress": 1000 },
      { "id": 1003, "ingress": 1000 },
      { "id": 1004 }
    ],
    "topics": [
      { "name": "MyTopicA", "partition": 0, "broker": 1001 },
      { "name": "MyTopicB", "partition": 1, "broker": 1002, "type": "leader" },
      { "name": "MyTopicC", "partition": 2, "broker": 1003, "type": "follower" }
    ]
}
```

## 新增/覆蓋 throttle 設定

```shell
POST /throttles
```

cURL 範例

```shell
curl -X POST http://localhost:8001/throttles \
    -H "Content-Type: application/json" \
    -d '{
      "brokers": [
        { "id":  1001, "ingress":  1000, "egress":  1000 },
        { "id":  1002, "ingress":  1000 }
      ],
      "topics": [
        { "name": "MyTopicA" },
        { "name": "MyTopicB", "partition": 2 },
        { "name": "MyTopicC", "partition": 3, "broker": 1001 },
        { "name": "MyTopicD", "partition": 4, "broker": 1001, "type": "leader" }
      ]
    }'
```

JSON Request 格式

| 名稱    | 說明                                     | 預設值 |
| ------- | ---------------------------------------- | ------ |
| brokers | (選填) 敘述要更新的 Broker Throttle 設定 | 無     |
| topics  | (選填) 敘述要更新的 Topic Throttle 設定  | 無     |

brokers 每個資料欄位

| 名稱      | 說明                                                                                                                                    | 預設值 |
|---------|---------------------------------------------------------------------------------------------------------------------------------------| ------ |
| id      | (必填) 欲更新節點的 id                                                                                                                        | 無     |
| ingress | (選填) 特定 Kafka 節點的 replication 流入流量的限制(單位 bytes/sec)，附註只有 `topics` 中有記錄到的對象，其 replication 流量會受限制，如果此欄位不存在則代表此節點的 replication 流入流量沒有被修改。 | 無     |
| egress  | (選填) 特定 Kafka 節點的 replication 流出流量的限制(單位 bytes/sec)，附註只有 `topics` 中有記錄到的對象，其 replication 流量會受限制。                                      | 無     |

topics 每個資料欄位

| 名稱      | 說明                                                         | 預設值 |
| --------- | ------------------------------------------------------------ | ------ |
| name      | (必填) 要套用 repliaction throttle 的 topic 名稱             | 無     |
| partition | (選填) 要套用 replication throttle 的 partition 編號，如果沒有指定，所有既有 partition 都會被套用 | 無     |
| broker    | (選填) 要套用 replication throttle 的 replica (所在之 broker)，如果沒有指定，所有既有的 replicas 都會被套用 | 無     |
| type      | (選填) 要套用 replication throttle 的 replica 身份，此值可以是 `leader` 或是 `follower`，如果沒有指定，則二者都會被套用 | 無     |

topic 描述格式只支援 `name`, `name, partition`, `name, partition, broker`, `name, partition, broker, type` 這四種 key 的組合，目前此 API 不支援其他種類的組合，比如 `name, type`。當給與這類型的組合，API 會回傳錯誤。

JSON Response 範例

- `brokers`: 回傳被此 POST 影響的結果。
  - `id`: 被影響的節點的編號。
  - `ingress`: 如果此欄位不存在則代表此節點的 replication 流入流量沒有被修改。
  - `egress`: 如果此欄位不存在則代表此節點的 replication 流出流量沒有被修改。
- `topics`: 被此 POST 影響的 logs。
  - `name`: 套用 replication 限流的 topic。
  - `partition`: 套用 replication 限流的 partition。
  - `broker`: 套用 replication 限流的 broker ID。
  - `type`: 套用的 log 身份，可以是 `leader` 或 `follower`，如果沒有標記這個欄位，意味著二者皆是。

```json
{
  "brokers": [
    { "id":  1001, "ingress":  1000, "egress":  1000 },
    { "id":  1002, "ingress":  1000 }
  ],
  "topics": [
    { "name":  "MyTopicA", "partition": 0, "broker": 1001, "type": "leader" },
    { "name":  "MyTopicA", "partition": 0, "broker": 1002, "type": "follower" },
    { "name":  "MyTopicA", "partition": 0, "broker": 1003, "type": "follower" },
    { "name":  "MyTopicA", "partition": 1, "broker": 1002, "type": "leader" },
    { "name":  "MyTopicA", "partition": 1, "broker": 1003, "type": "follower" },
    { "name":  "MyTopicA", "partition": 1, "broker": 1001, "type": "follower" },
    { "name":  "MyTopicB", "partition": 2, "broker": 1003, "type": "leader" },
    { "name":  "MyTopicB", "partition": 2, "broker": 1001, "type": "follower" },
    { "name":  "MyTopicC", "partition": 3, "broker": 1001 },
    { "name":  "MyTopicD", "partition": 4, "broker": 1001, "type": "leader" }
  ]
}
```

## 刪除 throttle 設定

```
DELETE /throttles
```

cURL 範例

* ##### 移除整個 Topic 的 replication throttle

  ```shell
  # 移除與 TopicA 相關的所有 replication throttle
  curl -X DELETE "http://localhost:8001/throttles?topic=MyTopicA"
  ```

* ##### 移除整個 Topic/Partition 的 replication throttle

  ```shell
  # 移除與 TopicA/partition 3 相關的所有 replication throttle
  curl -X DELETE "http://localhost:8001/throttles?topic=MyTopicA&partition=3"
  ```

* ##### 移除整個 Topic/Partition/Replica 的 replication throttle

  ```shell
  # 移除與 TopicA/partition 3/replica at broker 4 相關的所有 replication throttle
  curl -X DELETE "http://localhost:8001/throttles?topic=MyTopicA&partition=3&replica=4"
  ```

* ##### 移除特定 Topic/Partition/Replica 針對 leader 或是 follower 的 replication throttle

  ```shell
  # 移除 TopicA/partition 3/replica at broker 4 的 leader replication throttle
  curl -X DELETE "http://localhost:8001/throttles?topic=MyTopicA&partition=3&replica=4&type=leader"
  ```

* ##### 移除特定節點的 replication 輸入限流

  ```shell
  # 移除編號 1001 節點的輸入流量限流
  curl -X DELETE "http://localhost:8001/throttles?broker=1001&type=ingress"
  ```

* ##### 移除特定節點的 replication 輸出限流

  ```shell
  # 移除編號 1001 節點的輸出流量限流
  curl -X DELETE "http://localhost:8001/throttles?broker=1001&type=egress"
  ```

* ##### 移除特定節點的 replication 輸出和輸入限流

  ```shell
  # 移除編號 1001 節點的輸出和輸入流量限流
  curl -X DELETE "http://localhost:8001/throttles?broker=1001&type=ingress+egress"
  ```

  