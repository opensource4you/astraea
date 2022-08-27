/balancer
===

- [查詢更好的 partitions 配置](#查詢更好的-partitions-配置)

## 查詢更好的 partitions 配置
```shell
GET /balancer
```

參數

| 名稱     | 說明                   | 預設值                      |
|--------|----------------------|--------------------------|
| limit  | (選填) 要嘗試幾種組合         | 10000                    |
| topics | (選填) 只嘗試搬移指定的 topics | 無，除了內部 topics 以外的都作為候選對象 |

cURL 範例
```shell
curl -X GET http://localhost:8001/balancer
```

JSON Response 範例
- `cost`: 目前叢集的成本 (越高越不好)
- `newCost`: 評估後比較好的成本 (<= `cost`)
- `limit`: 嘗試了幾種組合
- `function`: 用來評估品質的方法
- `changes`: 新的 partitions 配置
  - `topic`: topic 名稱
  - `partition`: partition id
  - `before`: 原本的配置
    - `brokerId`: 有掌管 replica 的節點 id
    - `directory`: replica 存在資料的路徑
    - `size`: replica 在硬碟上的資料大小
  - `after`: 比較好的配置
```json
{
  "cost": 0.04948716593053935,
  "newCost": 0.04948716593053935,
  "limit": 10000,
  "function": "ReplicaLeaderCost",
  "changes": [
    {
      "topic": "__consumer_offsets",
      "partition": 40,
      "before": [
        {
          "brokerId": 1006,
          "directory": "/tmp/log-folder-0",
          "size": 1234
        }
      ],
      "after": [
        {
          "brokerId": 1002,
          "directory": "/tmp/log-folder-0"
        }
      ]
    },
    {
      "topic": "__consumer_offsets",
      "partition": 44,
      "before": [
        {
          "brokerId": 1003,
          "directory": "/tmp/log-folder-0",
          "size": 12355
        }
      ],
      "after": [
        {
          "brokerId": 1001,
          "directory": "/tmp/log-folder-2"
        }
      ]
    }
  ]
}
```
