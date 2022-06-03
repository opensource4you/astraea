/brokers
===

- [查詢所有 brokers](#查詢所有-brokers)
- [查詢指定 broker](#查詢指定-broker)

## 查詢所有 brokers
```shell
GET /brokers
```

cURL 範例
```shell
curl -X GET http://localhost:8001/brokers
```

JSON Response 範例
- `id`: broker id
- `topics`: 此 broker 下所有 topic
  - `topic`: topic 名稱
  - `partitionCount`: 此 topic 所擁有的 partition 數量
- `configs`: 此 broker 所設定的所有 configs
```json
{
  "brokers": [
    {
      "id": 1001,
      "topics": [
        {
          "topic": "test1",
          "partitionCount": 1
        }
      ],
      "configs": {
        "log.cleaner.min.compaction.lag.ms": "0",
        "metric.reporters": "",
        ...
      }
    }
  ]
}
```

## 查詢指定 broker
```shell
GET /brokers/{brokerId}
```

cURL 範例

查詢 id 為 1001 的 broker 資訊
```shell
curl -X GET http://localhost:8001/brokers/1001
```

JSON Response 範例
```json
{
  "id": 1001,
  "topics": [
    {
      "topic": "test1",
      "partitionCount": 1
    }
  ],
  "configs": {
    "log.cleaner.min.compaction.lag.ms": "0",
    "metric.reporters": "",
    ...
  }
}
```