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
- `topicName`: 正在新增的 replica 所屬於的 topic
- `partition`: 正在新增的 replica 所屬於的 partition
- `broker`: 正在新增的 replica 位於的節點
- `path`: 正在新增的 replica 位於的目錄
- `size`: 正在新增的 replica 大小
- `leaderSize`: 正在新增的 replica 最終的大小
- `progress`: 當前 replicas 搬移進度，以百分比顯示

```json
{
  "reassignments": [
    {
      "topicName": "chia",
      "partition": 0,
      "broker": 1,
      "path": "/tmp/log-folder-0",
      "size": 200,
      "leaderSize": 400,
      "progress": "50.00%"
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

## 排除指定節點的 replicas

```shell
POST /reassignments
```
參數

| 名稱      | 說明                   | 預設                     |
|---------|----------------------|------------------------|
| exclude | (必填) 指定排除之 broker id | 無                      |
| topic   | (選填) topic 名稱，排除該節點下指定 topic 的 partitions       | 無，代表排除該節點所有 partitions |

cURL 範例

排除 broker = 1003 身上屬於 "chia" 的 partitions
```shell
curl -X POST http://localhost:8001/reassignments \
    -H "Content-Type: application/json" \
    -d '{"plans": [{
    "exclude": 1003,
    "topic": "chia"
    }]}' 
```