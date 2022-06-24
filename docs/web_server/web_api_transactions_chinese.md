/transactions
===

- [查詢所有 transactions](#查詢所有-transactions)
- [查詢指定 transaction](#查詢指定-transaction)

## 查詢所有 transactions
```shell
GET /transactions
```

cURL 範例
```shell
curl -X GET http://localhost:8001/transactions
```

JSON Response 範例
- `id`: transaction id
- `coordinatorId`: 負責協調此次transaction行為的節點id (也就是broker id)
- `states`: 目前交易的狀態
- `producerId`: producer id，從 0 開始遞增
- `producerEpoch`: producer epoch，從 0 開始遞增，用以保證 producer transaction 的唯一性
- `transactionTimeoutMs`: 此次交易可持續多久
- `topicPartitions`: 此次交易涉及的partitions
```json
{
  "transactions": [
    {
      "id": "abc",
      "coordinatorId": 1001,
      "state": "Ongoing",
      "producerId": 0,
      "producerEpoch": 0,
      "transactionTimeoutMs": 60000,
      "topicPartitions": [
        {
          "topic": "testPerformance-1656075398393",
          "partition": 0
        }
      ]
    }
  ]
}
```

## 查詢指定 transaction

```shell
GET /transactions/{transaction id}
```

cURL 範例

查詢 abc 的交易行為
```shell
curl -X GET http://localhost:8001/transactions/abc
```

JSON Response 範例
 ```json
{
  "id": "abc",
  "coordinatorId": 1001,
  "state": "CompleteCommit",
  "producerId": 0,
  "producerEpoch": 0,
  "transactionTimeoutMs": 60000,
  "topicPartitions": []
}
 ```
