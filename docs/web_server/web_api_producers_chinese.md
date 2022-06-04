/producers
===

- [查詢 producers](#查詢-producers)

## 查詢 producers
```shell
GET /producers
```

參數

| 名稱        | 說明                |
|-----------|-------------------|
| topic     | (選填) topic 名稱     |
| partition | (選填) partition id |
- 僅能指定查詢 topic 或者 topic + partition，其他狀況均會回傳全部結果

cURL 範例

查詢所有 producers
```shell
curl -X GET http://localhost:8001/producers
```

查詢所有 producers 其 topic = test1 且 partition = 0
```shell
curl -X GET http://localhost:8001/producers?topic=test1&partition=0
```

JSON Response 範例
- `topic`: topic 名稱
- `partition`: partition id，從 0 開始遞增
- `states`: 當前活躍（未過期）的 producers 狀態
  - `producerId`: producer id，從 0 開始遞增
  - `producerEpoch`: producer epoch，從 0 開始遞增，用以保證 producer transaction 的唯一性
  - `lastSequence`: producer 提交於此 partition 最新 sequence number，從 0 開始遞增，搭配 producer id 來處理冪等性，保證單個 partition 不會重複寫入資料
  - `lastTimestamp`: producer 提交到此 partition 最新一筆資料的時間戳記

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