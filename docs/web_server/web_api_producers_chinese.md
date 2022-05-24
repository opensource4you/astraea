/producers
===

- [查詢 producers](#查詢 producers)

## 查詢 producers
```shell
GET /producers
```

參數

| 名稱        | 說明                |
|-----------|-------------------|
| topic     | (選填) topic 名稱     |
| partition | (選填) partition id |


cURL 範例

查詢所有 producers
```shell
curl -X GET http://localhost:8001/producers
```

查詢所有 topic 為 test1 且 partition 為 0 的 producers
```shell
curl -X GET http://localhost:8001/producers?topic=test1&partition=0
```

JSON Response 範例
```json
{
  "partitions": [
    {
      "topic": "test1",
      "partition": 0,
      "states": [
        {
          "producerId": 0,
          "producerEpoch": 0,
          "lastSequence": 0,
          "lastTimestamp": 1653578001967
        },
        {
          "producerId": 1,
          "producerEpoch": 0,
          "lastSequence": 0,
          "lastTimestamp": 1653578023368
        }
      ]
    }
  ]
}
```