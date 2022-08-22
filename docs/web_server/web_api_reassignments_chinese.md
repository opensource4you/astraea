/reassignments
===

- [查詢所有 reassignments](#查詢所有-reassignments)
- [變更 replicas 的節點部署](#變更-replicas-的節點部署)

## 查詢所有 reassignments
```shell
GET /reassignments
```

cURL 範例
```shell
curl -X GET http://localhost:8001/reassignments
```

JSON Response 範例
- `topicName`: 有 partitions 正在重新配置的 topic 名稱
- `partition`: 有 replicas 正在重新配置的 partition id
- `from`: replica 原本的位址
  - `broker`: broker id
  - `path`: 存放的資料夾路徑
- `to`: replica 將來的位址
```json
{
  "reassignments": [
    {
      "topicName": "chia",
      "partition": 0,
      "from": [
        {
          "broker": 1002,
          "path": "/tmp/log-folder-0"
        }
      ],
      "to": [
        {
          "broker": 1001,
          "path": "/tmp/log-folder-1"
        }
      ]
    }
  ]
}
```

## 變更 replicas 的節點部署

```shell
POST /reassignments
```
參數

| 名稱        | 說明                                  | 預設  |
|-----------|-------------------------------------|-----|
| topic     | (必填) topic 名稱                       | 無   |
| partition | (必填) partition id                   | 無 |
| to        | (必填) 新的部署位置，以 broker id 為值，並且要是陣列型別 | 無 |

cURL 範例

將 chia-0 這個 partition 的部署改變成只部署在 broker = 1003 這台身上
```shell
curl -X POST http://localhost:8001/reassignments \
    -H "Content-Type: application/json" \
    -d '"plans":[{
    "topic": "chia", 
    "partition": 0,
    "to": [1003]
    }]' 
```

## 變更 replica 的資料路徑

```shell
POST /reassignments
```
參數

| 名稱        | 說明                                   | 預設  |
|-----------|--------------------------------------|-----|
| topic     | (必填) topic 名稱                        | 無   |
| partition | (必填) partition id                    | 無 |
| broker    | (必填) replica 目前所在的節點                 | 無 |
| to        | (必填) 新的用來存放資料的路徑，該資料夾必須是上述節點可用的資料夾位置 | 無 |

cURL 範例

將 broker = 1003 上的 chia-0 這個 partition 的資料存放路徑移動到 /tmp/data
```shell
curl -X POST http://localhost:8001/reassignments \
    -H "Content-Type: application/json" \
    -d '"plans":[{
    "topic": "chia", 
    "partition": 0,
    "broker": 1003
    "to": "/tmp/data"
    }]' 
```