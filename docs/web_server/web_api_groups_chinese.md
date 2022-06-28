/groups
===

Consumer groups 是 Kafka 提供給讀取端的負載平衡機制，該機制允許我們建立多個 consumer instances，它們會透過 Kafka broker 來互相溝通，
並且協調各自該負責讀取哪些 partitions，藉此達到平衡以及容錯等功能。 Groups APIs 可以幫助我們觀察 groups 內各個 consumer member 的狀態

- [查詢所有 groups](#查詢所有-groups)
- [查詢指定 group](#查詢指定-group)
- [清除 members](#清除-members)

## 查詢所有 groups
```shell
GET /groups
```

參數

| 名稱                       | 說明                                   | 預設  |
|--------------------------|--------------------------------------|-----|
| topic                    | (選填) 只查詢跟此 topic 有關的 consumer groups | 無   |

cURL 範例
```shell
curl -X GET http://localhost:8001/groups
```

JSON Response 範例
- `memberId`: consumer 加入群組時由 (broker) coordinator所給予的唯一值
- `clientId`: consumer 送出請求時使用的名稱，使用者可以自定義，預設值為隨機字串
- `host`: consumer 運行的位址
- `offsetProgress`: consumer 目前消化資料的進度
  - `topic`: consumer 提取資料的 topic 名稱
  - `partitionId`: consumer 提取資料的 partition id
  - `earliest`: partition 現存的資料最早的紀錄位置
  - `current`: consumer 於 partition 目前處理的數據位置
  - `latest`: partition 最新紀錄位置
```json
{
  "groups": [
    {
      "groupId": "groupId-1653924298342",
      "members": []
    },
    {
      "groupId": "group-1",
      "members": [
        {
          "memberId": "consumer-group-1-1-22b93f1b-0eae-48ef-95f6-f562bf6769c0",
          "clientId": "consumer-group-1-1",
          "host": "/172.17.0.1",
          "offsetProgress": [
            {
              "topic": "test1",
              "partitionId": 0,
              "earliest": 0,
              "current": 2,
              "latest": 8
            }
          ]
        }
      ]
    }
  ]
}
```

## 查詢指定 group
```shell
GET /groups/{groupId}
```

參數

| 名稱                       | 說明                                   | 預設  |
|--------------------------|--------------------------------------|-----|
| topic                    | (選填) 只查詢跟此 topic 有關的 consumer groups | 無   |

cURL 範例
查詢名為 group-1 的 group 資訊
```shell
curl -X GET http://localhost:8001/groups/group-1
```

JSON Response 範例
```json
{
  "groupId": "group-1",
  "members": [
    {
      "memberId": "consumer-group-1-1-d0370b05-99e8-4d15-88ef-6da2d57e84fb",
      "clientId": "consumer-group-1-1",
      "host": "/172.17.0.1",
      "offsetProgress": [
        {
          "topic": "test1",
          "partitionId": 0,
          "earliest": 0,
          "current": 0,
          "latest": 0
        }
      ]
    }
  ]
}
```

## 清除 members
```shell
DELETE /groups/{groupId}?{instance=id}
```

參數

| 名稱                       | 說明                                             | 預設  |
|--------------------------|------------------------------------------------|-----|
| groupId                    | (必填) 清除此group底下所有的consumer members             | 無   |
| instance                    | (選填) 只清除此group instance id 關聯的 consumer member | 無   |

cURL 範例
刪除 group-1 底下所有的 consumer members
```shell
curl -X DELETE http://localhost:8001/groups/group-1
```
