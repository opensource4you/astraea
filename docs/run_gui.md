### Astraea GUI

Astraea 提供簡單但實用的 Kafka GUI 工具，讓使用者方便調閱和查詢 Kafka 內部常用的資訊。 Astraea GUI 有幾個特色：

1. 安裝簡單，使用者只需要先安裝 Java 11 + JavaFx，接著就能下載單一執行檔開始操作
2. 操作簡單，每個標籤清楚顯示會呈現的資訊、同時搭配關鍵字搜尋快速調閱相關資訊
3. 原理簡單，全程使用 Kafka 官方 APIs

#### 功能畫面連結

- [設定連線資訊](#setting)
- [查詢節點狀態](#broker)
- [查詢節點 JVM metrics](#metrics)
- [查詢 topic 狀態](#topic)
- [查詢 partition 狀態](#partition)
- [查詢 topic/broker 的參數設定](#config)
- [查詢 consumer group 資訊](#consumer)
- [查詢 idempotent producer 資訊](#producer)
- [查詢 transaction 狀態](#transaction)
- [查詢正在移動的 replica 狀態](#moving-replica)
- [查詢 quotas](#quota)
- [建立 topic](#create-topic)
- [更新 broker 的參數](#update-broker)
- [更新 topic 參數或是增加 partition 數量](#update-topic)
- [更新 partition 的節點或是截斷 offset](#update-partition)
- [更新 quotas](#update-quota)
- [執行負載平衡](#balancer)

#### 使用 Astraea GUI

1. (如果環境已可運行`JavaFX`則跳過此步驟) 下載 [Azul JRE FX 11](https://www.azul.com/downloads/?version=java-11-lts&os=windows&architecture=x86-64-bit&package=jre-fx)

![download_jre](gui/download_jre.png)

2. 下載 `Astraea GUI` 可執行檔，進到[Astraea GUI 可執行檔頁面](https://github.com/skiptests/astraea/packages/1652248)後搜尋結尾為`-all.jar`的檔案

![download_gui](gui/download_gui.png)

3. 運行 `Astraea GUI`

```shell
{JRE_HOME}/bin/java -jar astraea-gui-0.0.1-20220928.102654-3-all.jar
```

#### 功能介紹

## setting

`setting` 用來設定目標叢集的連線資訊，分別有`bootstrap servers`和`jmx port`，前者提供我們 kafka 操作、後者提供 java metrics 操作

![setting](gui/setting.png)

## broker
`broker` 提供我們查詢節點的資訊，搜尋欄位可用來過濾`id`或是`host`

![broker](gui/broker.png)

## metrics
`metrics` 提供我們查詢 JVM metrics 的能力，上方具有類別選項，搜尋欄位可以用來搜尋指定的 metrics 名稱，例如下圖中要找尋帶有 Message 的 metrics

![metrics](gui/metrics.png)

## topic
`topic` 提供我們查詢 topic 的資訊，搜尋欄位可以用來過濾 topic 名稱，例如下圖搜尋名稱中帶有 test 的 topics

![topic](gui/topic.png)

## partition
`partition` 提供我們查詢 partition 的資訊，搜尋欄位可以用來過濾 topic 名稱，例如下圖搜尋 topic 名稱中帶有 test 的 partitions

![partition](gui/partition.png)

## config
`config` 提供我們查詢 broker/topic 的參數設定，搜尋欄位可用來過濾參數的名稱，如下圖要找 broker 中有關 thread 的參數

![config](gui/config.png)

## consumer
`consumer` 提供我們查詢 consumer groups 的資訊，搜尋欄位可用來過濾 group id 或是 topic 名稱，如下圖是顯示有訂閱 tina 的 consumers

![consumer](gui/consumer.png)

## producer
`producer` 提供我們查詢 producer 的資訊，搜尋欄位可用來過濾 topic 名稱，如下圖是顯示有寫資料到 tina 的 producers。注意，只有 idempotent producers 的資訊可供查詢

![producer](gui/producer.png)

## transaction
`transaction` 提供我們查詢 transaction 的資訊，搜尋欄位可用來過濾 topic 名稱或是 transaction id，如下圖是顯示有涉及 tina 的交易狀態

![transaction](gui/transaction.png)

## moving replica
`moving replica` 提供我們查詢正在移動的 replicas 資訊，搜尋欄位可用來過濾 topic 名稱，如下圖是顯示 tina 的 partitions 移動狀況

![moving_replica](gui/moving_replica.png)

## quota
`quota` 提供我們基於 `client id` 或是 `ip address` 來查詢對應的 `quotas`

![quota_client_id](gui/quota_client_id.png)
![quota_ip](gui/quota_ip.png)

## create topic
`create topic` 提供我們建立 topic 的能力，除了帶有 * 記號的欄位是必填以外，其他欄位都是選填

![create_topic](gui/create_topic.png)

## update broker
`update broker` 提供我們動態更新節點的能力，如下圖我們將節點 1001 的 num.network.threads 的數量調整至 9 個

![update_broker](gui/update_broker.png)

## update topic
`update topic` 提供我們動態更新 topic 的能力，如下圖，我們透過上方下拉式選單選擇目標，再將 ikea 的 partitions 數量增加至 20 個 

![update_topic](gui/update_topic.png)

## update partition
`update partition` 提供我們動態更新 partition 的能力，包含移動及截斷兩種服務

如下圖，將 partition [0, 1, 2, 3] 移動到 broker [1001, 1004, 1005]

![update_partition](gui/update_partition_move.png)

如下圖，將 partition [4, 5, 6] offset 截斷至 300

![update_partition](gui/update_partition_truncate.png)\

## update quota
`update quota` 提供我們動態針對不同資源設下使用上限，目前支援三種資源：連線數、寫入速度和讀取速度。如下圖分別針對 `producer` 和 `consumer` 設定讀寫上限

![quota_producer](gui/quota_producer.png)
![quota_consumer](gui/quota_consumer.png)

下圖則是針對指定 `ip` 限制它單位時間能建立的連線數量

![quota_connection](gui/quota_connection.png)

## balancer
`balance topic` 提供我們平衡叢集負載的能力，目前支援三種平衡策略，分別是 replica 數量、leader 數量、以及資料量。接下來以平衡 replica 數量為例：

#### 下圖是一個 replica 數量不平衡的叢集，可看到節點 1005 有著較多的 replicas
![before_balance_replica](gui/before_balance_replica.png)

#### 選擇以 replica 數量為目標平衡
![balance_replica](gui/balance_replica.png)

#### 下圖是平衡 replica 數量後的結果，可看到節點 1005 身上的 replicas 已經平衡到其他節點
![after_balance_replica](gui/after_balance_replica.png)