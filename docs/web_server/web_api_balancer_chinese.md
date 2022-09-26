/balancer
===

- [查詢更好的 partitions 配置](#查詢更好的-partitions-配置)
- [執行負載平衡計劃](#執行負載平衡計劃)
- [查詢負載平衡計劃執行進度](#查詢負載平衡計劃執行進度)

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
- `migrations`: 計算搬移計畫的成本
  * `function`: 用來評估成本的演算法
  * `totalCost`: 各個broker的成本總和
  * `cost`: 針對各個broker計算成本的改變
    * `brokerId`: 有掌管 replica 的節點 id
    * `cost`: 改變的量，負值表示移出，正值表示移入
  * `unit`: 成本的單位
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
## 執行負載平衡計劃

```shell
POST /balancer
```

cURL 範例

```shell
curl -X POST http://localhost:8001/throttles \
    -H "Content-Type: application/json" \
    -d '{
      "changes": [
        { 
          "topic": "my-topic", "partition": 0,
          "before": [
            { "brokerId": 1001, "directory": "/tmp/log-folder-0" },
            { "brokerId": 1002, "directory": "/tmp/log-folder-0" },
            { "brokerId": 1003, "directory"; "/tmp/log-folder-0 } 
          ],
          "after": [ 
            { "brokerId": 1001, "directory": "/tmp/log-folder-1" },
            { "brokerId": 1002, "directory": "/tmp/log-folder-2" },
            { "brokerId": 1003, "directory": "/tmp/log-folder-3" }
          ]
        },
        { 
          "topic": "my-topic", "partition": 1,
          "before": [
            { "brokerId": 1002, "directory": "/tmp/log-folder-1" }
          ],
          "after": [ 
            { "brokerId": 1001 }
          ]
        }
      ]
    }'
```

JSON Request 格式

| 名稱      | 說明                                              | 預設值 |
|---------|-------------------------------------------------| ------ |
| changes | (必填) 陣列，敘述一群 topic/partition 最終預期的 replica 分佈狀況 | 無     |

### `changes` 每個資料欄位

| 名稱        | 說明                                                                                                                                                      | 預設值 |
|-----------|---------------------------------------------------------------------------------------------------------------------------------------------------------| ------ |
| topic     | (必填) 變更針對的 topic                                                                                                                                        | 無     |
| partition | (必填) 變更針對的 partition                                                                                                                                    | 無     |
| before    | (選填) 預期計劃執行前的 replica 分佈狀況，此欄位給定時，將會在負載平衡計劃執行驗證對應 topic/partition 的 replica 分佈狀況是否相同，如果當前叢集的 replica 分佈和預期狀況不一，Web service 會拒絕執行計劃。如果此欄位沒有指定則不會有任何檢查發生。 | 無     |
| after     | (必填) 預期計劃執行後的 replica 分佈狀況                                                                                                                              | 無     |

### `before` 每個資料欄位

| 名稱        | 說明                                       | 預設值 |
|-----------|------------------------------------------| ------ |
| brokerId  | (必填) 此 replica 應該位在的哪個節點。                | 無     |
| directory | (選填) 此 replica 應該位在此節點的哪個 data directory | 無     |

### `after` 每個資料欄位

| 名稱        | 說明                                             | 預設值 |
|-----------|------------------------------------------------| ------ |
| brokerId  | (必填) 此 replica 在負載平衡後應該位在的哪個節點。                | 無     |
| directory | (選填) 此 replica 在負載平衡後應該位在此節點的哪個 data directory | 無     |

> ##### `before` 和 `after` 陣列中的位置存在特別含義
> `before` 和 `after` 欄位用 JSON 陣列描述一個 topic/partition 預期的 replica 分佈狀況，
> 其中第一個欄位會被解釋成特定 topic/partition 的 preferred leader，且在負載平衡執行後，
> 這個 preferred leader 會被內部計劃的執行邏輯變更為當前 partition 的 leader。
> 
> 從 JSON 陣列第二位開始預期都是這個 topic/partition 的 follower logs，特別注意目前內部實作
> 不保證這個 follower logs 的順序是否會一致地反映到 Apache Kafka 內部的儲存資料結構內。

JSON Response 範例

* `token`: 一段字串，描述這個被接受的負載平衡計劃的執行編號。後續可以用這個 token 和特定 [API](#查詢負載平衡計劃執行進度) 來查詢負載平衡計劃的執行進度。

```json
{ "token": "46ecf6e7-aa28-4f72-b1b6-a788056c122a" }
```

## 查詢負載平衡計劃執行進度

```shell
GET /balancer/{token}
```

cURL 範例

```shell
curl -X POST http://localhost:8001/balancer/{token}
curl -X POST http://localhost:8001/balancer/46ecf6e7-aa28-4f72-b1b6-a788056c122a
```

| 名稱     | 說明                 | 預設值 |
|--------|--------------------| ------ |
| token | (必填) 欲查詢的負載平衡計劃之代號 | 無     |

> 目前實作不保留 web service 程式過去啟動時所接受的負載平衡計劃進度與結果

JSON Response 範例

* `done`: 描述對應的負載平衡計劃是否執行完成。

```json
{ "done": true }
```
