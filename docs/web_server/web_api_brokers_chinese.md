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
- `id`: broker 在叢集中的識別碼
- `topics`: 此 broker 下所有 topic
  - `topic`: topic 名稱
  - `partitionCount`: 此 topic 有多少個 partitions 在這個節點身上
- `configs`: 此 broker 正在使用的參數。這個參數列表為下列項目的組合，啟動節點時所用的參數檔、系統內預設值以及使用者動態修改的參數
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