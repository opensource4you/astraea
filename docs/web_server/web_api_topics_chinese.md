/topics
===

- [建立 topic](#建立 topic)
- [查詢所有 topics](#查詢所有 topics)
- [查詢指定 topic](#查詢指定 topic)

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

## 查詢所有 topics
```shell
GET /topics
```

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
