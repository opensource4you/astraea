/balancer
===

- [排程搜尋新的負載平衡計劃](#排程搜尋新的負載平衡計劃)
- [執行負載平衡計劃](#執行負載平衡計劃)
- [查詢負載平衡計劃的狀態](#查詢負載平衡計劃的狀態)

## 排程搜尋新的負載平衡計劃
```shell
POST /balancer
```

參數

| 名稱                | 說明                                                         | 預設值                                                   |
|-------------------|------------------------------------------------------------|-------------------------------------------------------|
| topics            | (選填) 只嘗試搬移指定的 topics                                       | 無，除了內部 topics 以外的都作為候選對象                              |
| timeout           | (選填) 指定產生時間                                                | 3s                                                    |
| balancer          | (選填) 愈使用的負載平衡計劃搜尋演算法                                       | org.astraea.common.balancer.algorithms.GreedyBalancer |
| balancerConfig    | (選填) 搜尋演算法的實作細節參數，此為一個 JSON Object 內含一系列的 key/value String | 無                                                     |
| costWeights       | (選填) 指定要優化的目標以及權重                                          | ReplicaSizeCost,ReplicaLeaderCost權重皆為1                |
 | maxMigratedSize   | (選填) 設定最大可搬移的log size                                      | 無 　                                                   |
 | maxMigratedLeader | (選填) 設定最大可搬移的leader 數量                                     | 無                                                     |

cURL 範例
```shell
curl -X POST http://localhost:8001/balancer \
    -H "Content-Type: application/json" \
    -d '{ "timeout": "10s" ,
      "balancer": "org.astraea.common.balancer.algorithms.GreedyBalancer",
      "balancerConfig": {
        "shuffle.tweaker.min.step": "1",
        "shuffle.tweaker.max.step": "5"
      },
      "costWeights": [
        { "cost": "org.astraea.common.cost.ReplicaSizeCost", "weight": 1 },
        { "cost": "org.astraea.common.cost.ReplicaLeaderCost", "weight": 1 }
      ],
      "maxMigratedSize": "300MB",
      "maxMigratedLeader": "3"
    }'
```

JSON Response 範例
- `id`: 這個負載平衡計劃的編號，後續可以透過這個編號來查詢此計劃的狀態。

```json
{
  "id": "46ecf6e7-aa28-4f72-b1b6-a788056c122a"
}
```

> ##### 搜尋負載平衡計劃需要時間
> 透過 `POST /balancer` 發起搜尋後，由於演算法邏輯和叢集效能資訊收集因素，這個計劃可能會花上一段時間才會找到。
> `POST /balancer` 回傳的計劃 id 能夠透過 [GET /balancer/{id}](#查詢負載平衡計劃的狀態) 查詢其搜尋狀態，
> 如果其 response 欄位 `generated` 為 `true`，則代表此計劃已經完成搜尋，能夠被 `PUT /balancer` 執行。
> 嘗試執行一個還沒完成搜尋的負載平衡計劃會發生錯誤。

## 執行負載平衡計劃

```shell
PUT /balancer
```

cURL 範例

```shell
curl -X PUT http://localhost:8001/balancer \
    -H "Content-Type: application/json" \
    -d '{ "id": "46ecf6e7-aa28-4f72-b1b6-a788056c122a" }'
```

JSON Request 格式

| 名稱  | 說明                 | 預設值 |
|-----|--------------------|-----|
| id  | (必填) 欲執行的負載平衡計劃之編號 | 無   |

JSON Response 範例

* `id`: 被接受的負載平衡計劃編號。

```json
{ "id": "46ecf6e7-aa28-4f72-b1b6-a788056c122a" }
```

後續能用特定 [API](#查詢負載平衡計劃的狀態) 來查詢負載平衡計劃的執行進度。

> ##### 一個叢集同時間只能執行一個負載平衡計劃
> 嘗試對一個叢集同時套用多個負載平衡計劃會導致意外的結果，因此 `PUT /balancer` 被設計為：
> 同時間只能夠執行一個負載平衡計劃，嘗試執行多個負載平衡計劃，那只有一個請求會被接受，其他請求將會被拒絕。
> 注意 Web Service 只能夠避免對當前執行 process 的多個執行請求做有效預防。在執行計劃前 Web Service
> 會檢查是否有正在進行的 Partition Reassignment，如果有偵測到則意味着可能有其他負載平衡計劃正在運行。
> Web Service 在這個情況下也會拒絕執行負載平衡計劃。

## 查詢負載平衡計劃的狀態

```shell
GET /balancer/{id}
```

cURL 範例

```shell
# curl -X GET http://localhost:8001/balancer/{id}
curl -X GET http://localhost:8001/balancer/46ecf6e7-aa28-4f72-b1b6-a788056c122a
```

| 名稱  | 說明                 | 預設值 |
|-----|--------------------|-----|
| id  | (必填) 欲查詢的負載平衡計劃之代號 | 無   |

> 目前實作不保留 web service 程式過去啟動時所接受的負載平衡計劃進度與結果

> 當查詢的 `id` 沒有對應到任何負載平衡計劃，回傳的 HTTP Status Code 會是 `404`

JSON Response 範例

* `id`: 此 Response 所描述的負載平衡計劃之編號
* `phase`: 代表此負載平衡計劃狀態的字串，可能是下列任一值
  * `Searching`: 正在搜尋能使叢集變更好的負載平衡計劃
  * `NoSolutionFound`: 計劃搜尋結束且沒辦法找到能使叢集變更好的分佈方式
  * `SearchException`: 計劃搜尋過程發生例外，`exception` 或 stderr 應該有相關的錯誤訊息
  * `ReadyForExecution`: 計劃搜尋完成，有找到一個讓叢集變更好的分佈方式，等待使用者調度執行
  * `Executing`: 正在將負載平衡計劃套用至叢集
  * `ExecutionException`: 負載平衡計劃套用過程發生例外，`exception` 或 stderr 應該有相關的錯誤訊息
  * `Executed`: 此負載平衡計劃已經成功套用至叢集
* `exception`: 當負載平衡計劃發生結束時，其所附帶的錯誤訊息。如果沒有錯誤，此欄位會是 `null`，可能觸發錯誤的時間點包含：
  1. 搜尋負載平衡計劃的過程中發生錯誤 (此情境下 `phase` 會是 `SearchException`)
  2. 執行負載平衡計劃的過程中發生錯誤 (此情境下 `phase` 會是 `ExecutionException`)
* `config` 此優化計劃的搜尋參數設定
  * `balancer`: 此計劃生成所使用的搜尋算法實作
  * `function`: 用來評估叢集狀態之品質的方法
  * `timeoutMs`: 此優化計劃的搜尋上限時間
* `plan`: 此負載平衡計劃的詳細資訊，如果此計劃還沒生成，則此欄位會是 `null`
  * `changes`: 新的 partitions 配置
    * `topic`: topic 名稱
    * `partition`: partition id
    * `before`: 原本的配置
      * `brokerId`: 有掌管 replica 的節點 id
      * `directory`: replica 存在資料的路徑
      * `size`: replica 在硬碟上的資料大小
    * `after`: 比較好的配置
  * `migrations`: 計算搬移計畫的成本
    * `function`: 用來評估成本的演算法
    * `totalCost`: 各個broker的成本總和
    * `cost`: 針對各個broker計算成本的改變
      * `brokerId`: 有掌管 replica 的節點 id
      * `cost`: 改變的量，負值表示移出，正值表示移入
    * `unit`: 成本的單位

```json
{
  "id": "46ecf6e7-aa28-4f72-b1b6-a788056c122a",
  "calculated": true,
  "generated": true,
  "scheduled": true,
  "done": true,
  "config": {
    "balancer": "org.astraea.common.balancer.algorithms.GreedyBalancer",
    "function": "WeightCompositeClusterCost[{\"org.astraea.common.cost.NetworkEgressCost@69be333e\" weight 1.0}, {\"org.astraea.common.cost.NetworkIngressCost@6c5ec944\" weight 1.0}]",
    "timeoutMs": 10000
  },
  "plan": {
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
}
```

```json
{
  "id": "46ecf6e7-aa28-4f72-b1b6-a788056c122a",
  "calculated": false,
  "generated": false,
  "scheduled": false,
  "done": false,
  "exception": "org.apache.kafka.common.KafkaException: Failed to create new KafkaAdminClient",
  "config": { /* ... */ },
  "plan":{ /* ... */ }
}
```

> ##### `before` 和 `after` 陣列中的位置存在特別含義
> `before` 和 `after` 欄位用 JSON 陣列描述一個 topic/partition 預期的 replica 分佈狀況，
> 其中第一個欄位會被解釋成特定 topic/partition 的 preferred leader，且在負載平衡執行後，
> 這個 preferred leader 會被內部計劃的執行邏輯變更為當前 partition 的 leader。
>
> 從 JSON 陣列第二位開始預期都是這個 topic/partition 的 follower logs，特別注意目前內部實作
> 不保證這個 follower logs 的順序是否會一致地反映到 Apache Kafka 內部的儲存資料結構內。

> ##### 優化目標的權重設定注意事項
> 權重的分配在優化算法遇到必須做取捨的情況時很重要，設定不適合的權重，可能會讓算法往比較不重要的優化目標做取捨。
> 
> 假設現在有 3 個優化目標和他們各自的權重
> 1. 節點輸入流量 weight 1
> 2. 節點輸出流量 weight 1
> 3. 節點的 Leader 數量 weight 1
> 
> 上述在進行負載優化時，由於 Leader 的平衡和網路吞吐量一樣重要，可能會導致為了多兼容這個優化需求而喪失一些優化的機會。
> 如發現 Balancer 生成計劃的 `newScore` 分數沒辦法貼近 0，則其生成的計劃可能在三者之間做了某種程度的取捨。如果不希望
> 這些取捨發生，或願意對某些不重要的優化項目做取捨，可以考慮調低 Leader 數量平衡的權重值。

> ##### 解讀 `score` 的注意事項
> 1. `score` 和 `newScore` 之值代表一個叢集分佈接近最佳狀況的程度。
> 2. 不同負載平衡計劃之間的 `score` 分數沒有關聯(不能彼此比較)。
> 3. 同一筆平衡計劃所產生分數能夠反映叢集的好壞，即 `scoreA` > `scoreB` 有定義，其代表 B 負載分佈比 A 負載分佈在此特定情境下更好。


目前此 endpoint 僅能查詢負載平衡計劃是否完成，如想知道更細部的搬移進度，可考慮使用 [Web Service Reassignments API](web_api_reassignments_chinese.md) 查詢。