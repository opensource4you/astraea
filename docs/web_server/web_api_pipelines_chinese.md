/pipelines
===

Pipeline 是用來描述一個 kafka partition 的資料來源以及資料目的，它根據 Kafka 所追蹤的 consumer group 和
idempotent/transaction producer 的資訊來拼湊出每個 partitions 是被哪些 consumers/producers 所使用

- [查詢所有 pipelines](#查詢所有-pipelines)

## 查詢所有 pipelines
```shell
GET /pipelines
```

cURL 範例
```shell
curl -X GET http://localhost:8001/pipelines
```

參數

| 名稱                       | 說明                                        | 預設    |
|--------------------------|-------------------------------------------|-------|
| active                   | (選填) 是否只顯示活躍的 partitions (也就是 from/to 有值) | false |

JSON Response 範例
- `topicPartitions`
  - `topic`: topic 名稱
  - `partition`: partition id
  - `from`: 有哪些 producers 正在寫資料到這個 partition ，注意，只有啟動 idempotent/transaction producer 才會被kafka所紀錄
  - `to`: 有哪些 consumers 正在讀取資料，注意，只有使用 consumer group 的 consumers 才會被kafka所紀錄
```json
{
  "topicPartitions": [
    {
      "topic": "testPerformance-1656075398393",
      "partition": 0,
      "from": [
        {
          "producerId": 0,
          "producerEpoch": 0,
          "lastSequence": 64354,
          "lastTimestamp": 1656075698698
        }
      ],
      "to": []
    },
    {
      "topic": "testPerformance-1656075901856",
      "partition": 0,
      "from": [
        {
          "producerId": 1,
          "producerEpoch": 0,
          "lastSequence": 2244,
          "lastTimestamp": 1656075909846
        }
      ],
      "to": [
        {
          "groupId": "groupId-1656075902062",
          "memberId": "consumer-groupId-1656075902062-1-b39f51a9-8a2a-46d5-aa5d-8595a014978f",
          "clientId": "consumer-groupId-1656075902062-1",
          "host": "/172.17.0.1"
        }
      ]
    }
  ]
}
```