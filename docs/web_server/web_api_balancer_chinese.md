/balancer
===

- [計算新的負載平衡計劃](#計算新的負載平衡計劃)
- [執行負載平衡計劃](#執行負載平衡計劃)
- [查詢負載平衡計劃執行進度](#查詢負載平衡計劃執行進度)

## 計算新的負載平衡計劃
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
- `id`: 這個負載平衡計劃的編號，後續可以透過這個編號來執行此計劃
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
  "id": "46ecf6e7-aa28-4f72-b1b6-a788056c122a",
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

> ##### `before` 和 `after` 陣列中的位置存在特別含義
> `before` 和 `after` 欄位用 JSON 陣列描述一個 topic/partition 預期的 replica 分佈狀況，
> 其中第一個欄位會被解釋成特定 topic/partition 的 preferred leader，且在負載平衡執行後，
> 這個 preferred leader 會被內部計劃的執行邏輯變更為當前 partition 的 leader。
>
> 從 JSON 陣列第二位開始預期都是這個 topic/partition 的 follower logs，特別注意目前內部實作
> 不保證這個 follower logs 的順序是否會一致地反映到 Apache Kafka 內部的儲存資料結構內。

## 執行負載平衡計劃

```shell
POST /balancer
```

cURL 範例

```shell
curl -X POST http://localhost:8001/balancer \
    -H "Content-Type: application/json" \
    -d '{ "id": "46ecf6e7-aa28-4f72-b1b6-a788056c122a" }'
```

JSON Request 格式

| 名稱  | 說明                 | 預設值 |
|-----|--------------------| ------ |
| id  | (必填) 欲執行的負載平衡計劃之編號 | 無     |


JSON Response 範例

* `id`: 被接受的負載平衡計劃編號。後續可以用這個 id 和特定 [API](#查詢負載平衡計劃執行進度) 來查詢負載平衡計劃的執行進度。

```json
{ "id": "46ecf6e7-aa28-4f72-b1b6-a788056c122a" }
```

## 查詢負載平衡計劃執行進度

```shell
GET /balancer/{id}
```

cURL 範例

```shell
# curl -X GET http://localhost:8001/balancer/{id}
curl -X GET http://localhost:8001/balancer/46ecf6e7-aa28-4f72-b1b6-a788056c122a
```

| 名稱  | 說明                 | 預設值 |
|-----|--------------------| ------ |
| id | (必填) 欲查詢的負載平衡計劃之代號 | 無     |

> 目前實作不保留 web service 程式過去啟動時所接受的負載平衡計劃進度與結果

JSON Response 範例

* `done`: 描述對應的負載平衡計劃是否執行完成。

```json
{ "done": true }
```


目前此 endpoint 僅能查詢負載平衡計劃是否完成，如想知道更細部的搬移進度，可考慮使用 [Web Service Reassignments API](web_api_reassignments_chinese.md) 查詢。