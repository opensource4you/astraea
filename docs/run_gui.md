### Astraea GUI

Astraea 提供簡單但實用的 Kafka GUI 工具，讓使用者方便調閱和查詢 Kafka 內部常用的資訊。 Astraea GUI 有幾個特色：

1. 安裝簡單，使用者只需要先安裝 Java 11 + JavaFx，接著就能下載單一執行檔開始操作
2. 操作簡單，每個標籤清楚顯示會呈現的資訊、同時搭配關鍵字搜尋快速調閱相關資訊
3. 原理簡單，全程使用 Kafka 官方 APIs

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
1. `setting` 用來設定目標叢集的連線資訊，分別有`bootstrap servers`和`jmx port`，前者提供我們 kafka 操作、後者提供 java metrics 操作

![setting](gui/setting.png)

2. `broker` 提供我們查詢節點的資訊，搜尋欄位可用來過濾`id`或是`host`

![broker](gui/broker.png)

3. `metrics` 提供我們查詢 JVM metrics 的能力，搜尋欄位可以用來搜尋指定的 metrics 名稱，例如下圖中要找尋帶有 Message 的 metrics

![metrics](gui/metrics.png)

4. `topic` 提供我們查詢 topic 的資訊，搜尋欄位可以用來過濾 topic 名稱，例如下圖搜尋名稱中帶有 test 的 topics

![topic](gui/topic.png)

5. `partition` 提供我們查詢 partition 的資訊，搜尋欄位可以用來過濾 topic 名稱，例如下圖搜尋 topic 名稱中帶有 test 的 partitions

![partition](gui/partition.png)

6. `config` 提供我們查詢 broker/topic 的參數設定，搜尋欄位可用來過濾參數的名稱，如下圖要找 broker 中有關 thread 的參數

![config](gui/config.png)

7. `consumer` 提供我們查詢 consumer groups 的資訊，搜尋欄位可用來過濾 group id 或是 topic 名稱，如下圖是顯示有訂閱 tina 的 consumers

![consumer](gui/consumer.png)

8. `producer` 提供我們查詢 producer 的資訊，搜尋欄位可用來過濾 topic 名稱，如下圖是顯示有寫資料到 tina 的 producers。注意，只有 idempotent producers 的資訊可供查詢

![producer](gui/producer.png)

9. `transaction` 提供我們查詢 transaction 的資訊，搜尋欄位可用來過濾 topic 名稱或是 transaction id，如下圖是顯示有涉及 tina 的交易狀態

![transaction](gui/transaction.png)

10. `moving replica ` 提供我們查詢正在移動的 replicas 資訊，搜尋欄位可用來過濾 topic 名稱，如下圖是顯示 tina 的 partitions 移動狀況

![moving_replica](gui/moving_replica.png)

11. `create topic ` 提供我們建立 topic 的能力，除了帶有 * 記號的欄位是必填以外，其他欄位都是選填

![create_topic](gui/create_topic.png)

12. `update topic ` 提供我們動態更新 topic 的能力，如下圖我們將 ikea 的 partitions 數量增加至 20 個 

![update_topic](gui/update_topic.png)

13. `move topic ` 提供我們動態移動 topic 的能力，如下圖我們將 ikea 的 partitions 通通移動到節點 1008 和節點 1005

![move_topic](gui/move_topic.png)

14. `update broker ` 提供我們動態更新節點的能力，如下圖我們將節點 1002 的 num.network.threads 的數量調整至 9 個

![update_broker](gui/update_broker.png)

15. `balance topic ` 提供我們平衡叢集負載的能力，目前支援三種平衡策略，分別是 replica 數量、leader 數量、以及資料量。接下來以平衡 replica 數量為例：

#### 下圖是一個 replica 數量不平衡的叢集，可看到節點 1005 有著較多的 replicas
![before_balance_replica](gui/before_balance_replica.png)

#### 選擇以 replica 數量為目標平衡
![balance_replica](gui/balance_replica.png)

#### 下圖是平衡 replica 數量後的結果，可看到節點 1005 身上的 replicas 已經平衡到其他節點
![after_balance_replica](gui/after_balance_replica.png)