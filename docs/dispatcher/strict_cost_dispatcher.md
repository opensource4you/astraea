# Strict Cost Dispatcher

Strict Cost Dispatcher 是 [Astraea partitioner](./README.md) 之一，功能在於 "依據 broker cost" 來選擇發送的 partition 順序。"broker cost" 是使用者自定義的效能指標，目前使用 "request 平均延遲" 作為效能指標。（TODO: 允許使用者選擇效能指標）

### 於本專案的 [performance tool](../performance_benchmark.md) 使用

```bash
$ ./docker/start_app.sh performance --bootstrap.servers 192.168.103.26:9092 --partitioner org.astraea.common.partitioner.StrictCostDispatcher
```

（加入參數 "--partitioner org.astraea.app.partitioner.StrictCostDispatcher" 即可選定使用 Strict Cost Dispatcher 作為 partitioner ）

### 導入自己的程式

需在創建 Kafka Producer 時設定使用 `StrictCostDispatcher`。

```java
var props = new Properties();

/* ... Some Kafka properties to put into ...*/

props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.astraea.common.partitioner.StrictCostDispatcher");

var producer = new KafkaProducer<String, String>(props);
```

### 功能說明

Strict Cost Dispatcher 實做了 Apache Kafka 的 `org.apache.kafka.clients.producer.Partitioner` 介面，當 record 沒有指定要發送到哪個 partition 時，producer 便會呼叫 partitioner 來決定要把 record 發送到哪個 partition 上？

此 Dispatcher (or called partitioner) 便藉著 Apache Kafka 提供的這項自由度，來選擇 "適合" 的 partition 發送。Strict Cost Dispatcher 在選擇 partition 前，會

1. 獲取使用者定義的效能指標（預設是使用 producer 端的 request 平均延遲）
2. 使用該些效能指標計算各個 broker 的分數
3. 加權各個效能指標計算出的分數
4. 利用分數建立 [Smooth Round Robin](../../common/src/main/java/org/astraea/common/partitioner/RoundRobin.java) 的排序
5. 紀錄前 `ROUND_ROBIN_LENGTH` 筆排序並重複使用

以上5個步驟每 `round.robin.lease` 時間會重新計算一次，預設的時間是4秒。可以在傳入的 `Properties` 中設定，

```bash
# 使用 performance tool 時，設定更新效能指標的時間
$ ./docker/start_app.sh performance --bootstrap.servers 192.168.103.26:9092 --partitioner org.astraea.common.partitioner.StrictCostDispatcher --configs
```

```java
// 為 StrictCostDispatcher 設定更新效能使標的時間
props.put(StrictCostDispatcher.ROUND_ROBIN_LEASE_KEY, "10s");
```

[Smooth Round Robin](../../common/src/main/java/org/astraea/common/partitioner/RoundRobin.java) 會讓分數較高的節點有較高的出現頻率，但不會過於密集，讓我們在平衡負載的同時也顧及資料分散儲存處理。
