/topics
===

- [建立 topic](#建立-topic)
- [查詢所有 topics](#查詢所有-topics)
- [查詢指定 topic](#查詢指定-topic)

## 建立 topic
```shell
POST /topics
```

參數

| 名稱         | 說明                  | 預設值 |
|------------|---------------------|-----|
| name       | (必填) topic 名稱       | 無   |
| partitions | (選填) partition 數量   | 1   |
| replicas   | (選填) replication 數量 | 1   |
- replicas 數量須 <= brokers 數量

cURL 範例

建立名為 test1 且 partitions 為 1，replicas 為 1 的 topic
```shell
curl -X POST http://localhost:8001/topics \
    -H "Content-Type: application/json" \
    -d '{
    "name": "test1",
    "partitions": 1,
    "replicas": 1
    }'
```

所有在 JSON Response `configs` 裡頭的參數也可以透過此 api 來設定。範例如下
```shell
curl -X POST http://localhost:8001/topics \
    -H "Content-Type: application/json" \
    -d '{
    "name": "test1",
    "max.message.bytes": 1000
    }'
```

JSON Response 範例
- `name`: topic 名稱
- `partitions`: 該 topic 下所有 partitions
  - `id`: partition id，從 0 開始遞增
  - `earliest`: partition 現存的資料最早的紀錄位置
  - `latest`: partition 最新紀錄位置
  - `replicas`: partition 副本資料
    - `broker`: 儲存此副本的 broker 在叢集中的識別碼
    - `lag`: 副本同步狀況，計算公式為副本 LEO (Log End offset) - HW (High Watermark)，用以判斷副本最新資料與所有 ISR 資料水位比較
    - `size`: 副本資料大小，單位為 bytes
    - `leader`: 此副本所在的節點是否為 leader
    - `inSync`: 此副本所在的節點是否位於 ISR (In-Sync Replicas) 列表裡頭。每個 partition 都會有一個 ISR，由能夠和 leader 保持同步的 follower 與 leader 組成的集合 
    - `isFuture`: 此副本是否正準備由另一份新的副本取代，可能的觸發時機點為 reassign partitions，副本準備要搬移到不同節點，或者副本準備移到同節點的不同路徑下
    - `path`: 副本於節點中的路徑
- `configs`: 此 topic 正在使用的參數，這些參數都可以在建立 topic 時設定

```json
{
  "name": "test1",
  "partitions": [
    {
      "id": 0,
      "earliest": 0,
      "latest": 0,
      "replicas": [
        {
          "broker": 1001,
          "lag": 0,
          "size": 0,
          "leader": true,
          "inSync": true,
          "isFuture": false,
          "path": "/tmp/log-folder-0"
        }
      ]
    }
  ],
  "configs": {
    "compression.type": "producer",
    "leader.replication.throttled.replicas": "",
    "message.downconversion.enable": "true",
    "min.insync.replicas": "1",
    "segment.jitter.ms": "0",
    "cleanup.policy": "delete",
    "flush.ms": "9223372036854775807",
    "follower.replication.throttled.replicas": "",
    "segment.bytes": "1073741824",
    "retention.ms": "604800000",
    "flush.messages": "9223372036854775807",
    "message.format.version": "3.0-IV1",
    "max.compaction.lag.ms": "9223372036854775807",
    "file.delete.delay.ms": "60000",
    "max.message.bytes": "1048588",
    "min.compaction.lag.ms": "0",
    "message.timestamp.type": "CreateTime",
    "preallocate": "false",
    "min.cleanable.dirty.ratio": "0.5",
    "index.interval.bytes": "4096",
    "unclean.leader.election.enable": "false",
    "retention.bytes": "-1",
    "delete.retention.ms": "86400000",
    "segment.ms": "604800000",
    "message.timestamp.difference.max.ms": "9223372036854775807",
    "segment.index.bytes": "10485760"
  }
}
```

## 查詢所有 topics
```shell
GET /topics
```

參數

| 名稱           | 說明                                                                                             | 預設值                |
|--------------|------------------------------------------------------------------------------------------------|--------------------|
| partition    | (選填) 指定要查看哪一個 partition                                                                        | 無，代表全部 partitions  |
| listInternal | (選填) 為 boolean 值。若填寫 true，則會列出 kafka 內部所使用的 topics，如 __commit_offsets。若設為 false，則不會列出此種 topics | true，代表列出所有 topics |

cURL 範例
```shell
curl -X GET http://localhost:8001/topics
```

JSON Response 範例
```json
{
  "topics": [
    {
      "name": "test1",
      "partitions": [
        {
          "id": 0,
          "earliest": 0,
          "latest": 0,
          "replicas": [
            {
              "broker": 1001,
              "lag": 0,
              "size": 0,
              "leader": true,
              "inSync": true,
              "isFuture": false,
              "path": "/tmp/log-folder-0"
            }
          ]
        }
      ],
      "configs": {
        "compression.type": "producer",
        "leader.replication.throttled.replicas": "",
        "message.downconversion.enable": "true",
        "min.insync.replicas": "1",
        "segment.jitter.ms": "0",
        "cleanup.policy": "delete",
        "flush.ms": "9223372036854775807",
        "follower.replication.throttled.replicas": "",
        "segment.bytes": "1073741824",
        "retention.ms": "604800000",
        "flush.messages": "9223372036854775807",
        "message.format.version": "3.0-IV1",
        "max.compaction.lag.ms": "9223372036854775807",
        "file.delete.delay.ms": "60000",
        "max.message.bytes": "1048588",
        "min.compaction.lag.ms": "0",
        "message.timestamp.type": "CreateTime",
        "preallocate": "false",
        "min.cleanable.dirty.ratio": "0.5",
        "index.interval.bytes": "4096",
        "unclean.leader.election.enable": "false",
        "retention.bytes": "-1",
        "delete.retention.ms": "86400000",
        "segment.ms": "604800000",
        "message.timestamp.difference.max.ms": "9223372036854775807",
        "segment.index.bytes": "10485760"
      }
    }
  ]
}
```

## 查詢指定 topic

```shell
GET /topics/{topicName}
```

參數

| 名稱        | 說明                      | 預設值               |
|-----------|-------------------------|-------------------|
| partition | (選填) 指定要查看哪一個 partition | 無，代表全部 partitions |


cURL 範例

查詢名為 test1 的 topic 資訊
```shell
curl -X GET http://localhost:8001/topics/test1
```

JSON Response 範例
 ```json
{
  "name": "test1",
  "partitions": [
    {
      "id": 0,
      "earliest": 0,
      "latest": 0,
      "replicas": [
        {
          "broker": 1001,
          "lag": 0,
          "size": 0,
          "leader": true,
          "inSync": true,
          "isFuture": false,
          "path": "/tmp/log-folder-0"
        }
      ]
    }
  ],
  "configs": {
    "compression.type": "producer",
    "leader.replication.throttled.replicas": "",
    "message.downconversion.enable": "true",
    "min.insync.replicas": "1",
    "segment.jitter.ms": "0",
    "cleanup.policy": "delete",
    "flush.ms": "9223372036854775807",
    "follower.replication.throttled.replicas": "",
    "segment.bytes": "1073741824",
    "retention.ms": "604800000",
    "flush.messages": "9223372036854775807",
    "message.format.version": "3.0-IV1",
    "max.compaction.lag.ms": "9223372036854775807",
    "file.delete.delay.ms": "60000",
    "max.message.bytes": "1048588",
    "min.compaction.lag.ms": "0",
    "message.timestamp.type": "CreateTime",
    "preallocate": "false",
    "min.cleanable.dirty.ratio": "0.5",
    "index.interval.bytes": "4096",
    "unclean.leader.election.enable": "false",
    "retention.bytes": "-1",
    "delete.retention.ms": "86400000",
    "segment.ms": "604800000",
    "message.timestamp.difference.max.ms": "9223372036854775807",
    "segment.index.bytes": "10485760"
  }
}
 ```
## 刪除 topic
```shell
DELETE /topics/{topicName}
```

cURL 範例

```shell
curl -X DELETE "http://localhost:8001/topics/mytopic"
```