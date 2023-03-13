# Astraea 使用故障排除

這個條目記錄一些 Astraea 工具使用上可能會遭遇到的問題。

## Balancer 的 Data Directory 負載轉移失敗

Astraea Balancer 支援同節點 Data Director 的負載轉移。但由於 Apache Kafka 2.2.0 到 3.5.0 版本之間的一個 
[bug](https://issues.apache.org/jira/browse/KAFKA-9087)，這個負載轉移的過程有機會觸發一個節點狀態操作上的
race condition ，這最終會導致 Replica 搬移操作失敗。由於這個 bug，目前 WebService Balancer 預設是**不進行**
同節點下的 Data Directory 負載轉移。但使用者可以透過重寫特定設定來強制啟動 Data Directory 的負載轉移操作，
詳情參考[文件](./web_server/web_api_balancer_chinese.md#進行-Data-Directory-負載轉移的風險)。

### 症狀

如果 Data Directory 的負載轉移觸發了這個 bug，叢集會有下面這些症狀：

* 部分的 Replica 一直處於 `future` 狀態。
  * 對 Astraea WebService 發送 `GET /topics`，回傳結果指出有部分 Replica 的 `isFuture` 一直處於 `true`，
    且他的 `size` 總是維持在 `0`。
* 負責該 Replica 的 Apache Kafka Broker，其 log 有類似下者的錯誤：
    ```
    java.lang.IllegalStateException: Offset mismatch for the future replica topicB-121: fetched offset = 142138, log end offset = 0.
        at kafka.server.ReplicaAlterLogDirsThread.processPartitionData(ReplicaAlterLogDirsThread.scala:67)
        at kafka.server.AbstractFetcherThread.$anonfun$processFetchRequest$7(AbstractFetcherThread.scala:336)
        at scala.Option.foreach(Option.scala:437)
        at kafka.server.AbstractFetcherThread.$anonfun$processFetchRequest$6(AbstractFetcherThread.scala:325)
        at kafka.server.AbstractFetcherThread.$anonfun$processFetchRequest$6$adapted(AbstractFetcherThread.scala:324)
        at kafka.utils.Implicits$MapExtensionMethods$.$anonfun$forKeyValue$1(Implicits.scala:62)
        at scala.collection.MapOps.foreachEntry(Map.scala:244)
        at scala.collection.MapOps.foreachEntry$(Map.scala:240)
        at scala.collection.AbstractMap.foreachEntry(Map.scala:405)
        at kafka.server.AbstractFetcherThread.processFetchRequest(AbstractFetcherThread.scala:324)
        at kafka.server.AbstractFetcherThread.$anonfun$maybeFetch$3(AbstractFetcherThread.scala:124)
        at kafka.server.AbstractFetcherThread.$anonfun$maybeFetch$3$adapted(AbstractFetcherThread.scala:123)
        at scala.Option.foreach(Option.scala:437)
        at kafka.server.AbstractFetcherThread.maybeFetch(AbstractFetcherThread.scala:123)
        at kafka.server.AbstractFetcherThread.doWork(AbstractFetcherThread.scala:106)
        at kafka.utils.ShutdownableThread.run(ShutdownableThread.scala:96)
    ```

### 解決方法

首先需要知道遇到問題的 Replica 有哪些，而後對他們做修正，下面透過 WebService 來示範如何修正問題。

首先啟動 Astraea WebService，假設他的位置在 `localhost:8001`，接著執行：

```shell
curl -X GET --location "http://localhost:8001/topics" \
  | jq '[ .topics[] | {name, partitions: [.partitions[] | select(.replicas[].isFuture == true) | {id, replicas: [.replicas[]]}]} ]'
```

這個指令首先對 WebService 撈取特定 topics 的資訊，而後透過 `jq` 從所有 topics 中找出 `isFuture` 是 `true` 的那些 partition 有誰。
執行之後可能會看到類似下列的輸出

```json5
[
  {
    "name": "__consumer_offsets",
    "partitions": []
  },
  {
    "name": "topicA",
    "partitions": []
  },
  {
    "name": "topicB",
    "partitions": [
      {
        "id": 121,    // Partition 編號
        "replicas": [
          {
            "broker": 1,
            "inSync": true,
            "isFuture": true,   // <-----  isFuture == true
            "lag": 188858,
            "leader": false,
            "path": "/tmp/log-folder-2", // 負載優化要移動到的 data directory
            "size": 0           // <----- size 0
          },
          {
            "broker": 1,
            "inSync": true,
            "isFuture": false,
            "lag": 0,
            "leader": false,
            "path": "/tmp/log-folder-1", // 最初的 data directory
            "size": 325283565
          },
          {
            "broker": 2,
            "inSync": true,
            "isFuture": false,
            "lag": 0,
            "leader": true,
            "path": "/tmp/log-folder-0",
            "size": 325283565
          }
        ]
      },
      {
        "id": 221,    // Partition 編號
        "replicas": [
          {
            "broker": 1,
            "inSync": true,
            "isFuture": false,
            "lag": 0,
            "leader": true,
            "path": "/tmp/log-folder-2",
            "size": 386236916
          },
          {
            "broker": 4,
            "inSync": true,
            "isFuture": false,
            "lag": 0,
            "leader": false,
            "path": "/tmp/log-folder-0", // 最初的 data directory
            "size": 386236916
          },
          {
            "broker": 4,
            "inSync": true,
            "isFuture": true,   // <-----  isFuture == true
            "lag": 202107,
            "leader": false,
            "path": "/tmp/log-folder-1", // 負載優化要移動到的 data directory
            "size": 0           // <----- size 0
          }
        ]
      }
    ]
  }
]
```

上述輸出中，`topicB` 的部分有兩個 Partition 有遇到這個問題。 出問題的 Partition 分別是 `topicB-121` 和 `topicB-221`，
他們各自有個 Replica 其 `isFuture` 是 `true` 且 `size` 維持在 `0` 不動。
您可以順便去查看編號 1 和 4 的 節點 log，應該可以在他們的 log 中看到相關的錯誤訊息。

同一個 Replica 其 `isFuture` 為 `false` 那一份是套用負載優化前的原先位置，而 `isFuture` 是 `true` 那一份是優化計劃
預計要的移動到的位置，透過觀察我們可以統計出下面這些關係。

| TopicPartition | 節點  | 原本的資料夾            | 欲移動到的資料夾          |
|----------------|-----|-------------------|-------------------|
| topicB-121     | 1   | /tmp/log-folder-1 | /tmp/log-folder-2 |
| topicB-221     | 4   | /tmp/log-folder-0 | /tmp/log-folder-1 |

我們現在要嘗試手動抵消這個搬移操作，對 Web Service 發起類似下者 reassignment request，
指示那份 replica 應該要放回他們原先的位置。

```shell
curl -X POST --location "http://localhost:8001/reassignments" \
    -H "Content-Type: application/json" \
    -d '{
          "toFolders": [
            { "topic": "topicB", "partition": 121, "broker": 1, "to": "/tmp/log-folder-1"},
            { "topic": "topicB", "partition": 221, "broker": 4, "to": "/tmp/log-folder-0"}
          ]
        }'
```

上述的 HTTP Request 把有 isFuture 問題的 replica 移動回他們負載優化前的資料夾。

重複執行剛剛的指令

```shell
curl -X GET --location "http://localhost:8001/topics" \
  | jq '[ .topics[] | {name, partitions: [.partitions[] | select(.replicas[].isFuture == true) | {id, replicas: [.replicas[]]}]} ]'
```

原先 future 卡住的 replica 應該會消失不見。

```json
[
  {
    "name": "__consumer_offsets",
    "partitions": []
  },
  {
    "name": "topicA",
    "partitions": []
  },
  {
    "name": "topicB",
    "partitions": []
  }
]
```

再來，原先的負載優化計劃希望把

* `topicB-121` 在 broker 4 的 replica 搬移到 `/tmp/log-folder-2`
* `topicB-221` 在 broker 1 的 replica 搬移到 `/tmp/log-folder-1`

我們發起另外一個 reassignment request，將他們移回預期的位置。

```shell
curl -X POST --location "http://localhost:8001/reassignments" \
    -H "Content-Type: application/json" \
    -d '{
          "toFolders": [
            { "topic": "topicB", "partition": 121, "broker": 1, "to": "/tmp/log-folder-2"},
            { "topic": "topicB", "partition": 221, "broker": 4, "to": "/tmp/log-folder-1"}
          ]
        }'
```

如此一來即可解決這個 data directory 間的搬移的 bug。

## Balancer 計劃無法生成 (因 LogSize metric 不存在)

在 Apache Kafka 3.5.0 之前存在一個 [bug](https://issues.apache.org/jira/browse/KAFKA-14544)， Replica 經過搬移後，
其對應的 `kafka.log:type=Log,name=Size,topic={t},partition={p}` JMX MBean 有機會會不存在於目的地節點。由於某些 CostFunction
的運作需要叢集中所有 Replica 的 Log MBean 資訊才能運作，觸發這個問題時可能使 Cost Function 無法給叢集打分。

### 症狀

如果遇到這個情況，Balancer 針對和 `Log` 有關的 Cost Function (如 `NetworkIngrsesCost` 和 `NetworkEgressCost`) 
會因為沒辦法蒐集到足夠的 metrics 而無法進行後續的計劃搜尋。

### 解決辦法

重開服務該 Partition 的 Kafak 節點，重開後對應的 `Log` Mbean 會重新顯現。