# Strict Cost Dispatcher 測試

此次實驗目的是量測 `StrictCostDispatcher` "**有使用** interdependent message" 和 "**沒有使用** interdependent message" 對效能的影響。對使用 Astraea Partitioner 的使用者來說，也許有些 record 需要被送到同一個 partition ，因此 Astraea Partitioner 提供了 [interdependent](../README.md#astraea-dispatcher-interdependent-message-功能) 的機制。

## 測試環境

### 硬體規格

實驗使用6台實體機器，以下皆以代號表示，分別是 B1, B2, B3, C1, C2, C3 ，六台實體機器規格均相同

| 硬體       | 品名                                                         |
| ---------- | ------------------------------------------------------------ |
| CPU        | Intel i9-12900K 3.2G(5.2G)/30M/UHD770/125W                   |
| 主機板     | 微星 Z690 CARBON WIFI(ATX/1H1P/Intel 2.5G+Wi-Fi 6E)          |
| 記憶體     | 十銓 T-Force Vulcan 32G(16G*2) DDR5-5200 (CL40)              |
| 硬碟       | 威剛XPG SX8200Pro 2TB/M.2 2280/讀:3500M/寫:3000M/TLC/SMI控 * 2 |
| 散熱器     | NZXT Kraken Z53 24cm水冷排/2.4吋液晶冷頭/6年/厚:5.6cm        |
| 電源供應器 | 海韻 FOCUS GX-850(850W) 雙8/金牌/全模組                      |
| 網卡       | Marvell AQtion 10Gbit Network Adapter                        |

### 網路拓樸

```
          switch(10G)
┌─────┬─────┬─────┬─────┬─────┐
B1    B2    B3    C1    C2    C3
```

### 軟體版本

| 軟體                   | 版本(/image ID)                          |
| ---------------------- | ---------------------------------------- |
| 作業系統               | ubuntu-20.04.3-live-server-amd64         |
| Astraea revision       | 08b4e32f31091a3de69775db5442eb631deca550 |
| Zookeeper version      | 3.7.1                                    |
| Apache Kafka version   | 3.2.1                                    |
| Java version           | OpenJDK 11                               |
| Docker version         | 20.10.17, build 100c701                  |
| grafana image ID       | b6ea013786be                             |
| prometheus version     | v2.32.1                                  |
| node-exporter image ID | 1dbe0e931976                             |

作業系統硬碟切割

| 硬碟   | partition1 | partition2                   |
| ------ | ---------- | ---------------------------- |
| 硬碟一 | 50G /      | (rest) (Kafka log directory) |
| 硬碟二 | 50G /home  | (rest) (Kakfa log directory) |

實驗執行軟體

| 執行軟體                 |  B1  |  B2  |  B3  |  C1  |  C2  |  C3  |
| ------------------------ | :--: | :--: | :--: | :--: | :--: | :--: |
| Zookeeper                |  V   |      |      |      |      |      |
| Kakfa Broker             |  V   |  V   |  V   |      |      |      |
| Node Exporter            |  V   |  V   |  V   |      |      |      |
| Prometheus               |      |      |  V   |      |      |      |
| Grafana                  |      |      |  V   |      |      |      |
| Astraea Performance tool |      |      |      |  V   |  V   |  V   |

## 測試情境

使用 3 台伺服器做 broker、3 台做發送端，使用 [performance tool](../../performance_benchmark.md) 來測量 producer 的發送吞吐與延遲，比較不同 interdependent size 的影響。

測試方式是：實驗進行 3 次，開 60 partitions 的 topic 。在 C1, C2, C3 上，使用 [Astraea performance tool](../../performance_benchmark.md) 發送資料，設計上是每 n 筆 record 會發送到同一個 partition 上，n 是使用者指定的數，這裡分別使 n = 1, 10, 100，觀察 [Astraea performance tool](../../performance_benchmark.md) 輸出的吞吐量與平均發送延遲。

1. `--interdependent.size 1`
2. `--interdependent.size 10`
3. `--interdependent.size 100`

```bash
# Run StrictCostDispatcher with no interdependent
REVISION=08b4e32f31091a3de69775db5442eb631deca550 docker/start_app.sh performance \
--bootstrap.servers 192.168.103.185:9092,192.168.103.186:9092,192.168.103.187:9092 \
--value.size 10KiB \
--producers 4 \
--consumers 0 \
--run.until 30m \
--topics testing \
--report.path /home/kafka/hong/report \
--interdependent.size 1 \
--partitioner org.astraea.common.partitioner.StrictCostDispatcher

# Run StrictCostDispatcher with interdependent 10 records
REVISION=08b4e32f31091a3de69775db5442eb631deca550 docker/start_app.sh performance \
--bootstrap.servers 192.168.103.185:9092,192.168.103.186:9092,192.168.103.187:9092 \
--value.size 10KiB \
--producers 4 \
--consumers 0 \
--run.until 30m \
--topics testing \
--report.path /home/kafka/hong/report \
--interdependent.size 10 \
--partitioner org.astraea.common.partitioner.StrictCostDispatcher

# Run StrictCostDispatcher with interdependent 100 records
REVISION=08b4e32f31091a3de69775db5442eb631deca550 docker/start_app.sh performance \
--bootstrap.servers 192.168.103.185:9092,192.168.103.186:9092,192.168.103.187:9092 \
--value.size 10KiB \
--producers 4 \
--consumers 0 \
--run.until 30m \
--topics testing \
--report.path /home/kafka/hong/report \
--interdependent.size 100 \
--partitioner org.astraea.common.partitioner.StrictCostDispatcher
```

## 測試結果

從平均吞吐的折線圖來觀察，吞吐量平均都超過 2000 (MiB/sec) 。觀察到有使用 `interdependent` 的吞吐量較高 (約11%)。

從平均延遲來看，使用 `interdependent` 的延遲會降低，理應 `StrictCostDispatcher` 預設是對延遲做優化，那麼多了 `interdependent` 的限制不應該讓延遲變得更好，也說明了 `StrictCostDispatcher` 有改善的空間。（這裡紀錄的延遲是 "moving average" 的一種，每秒取的 "moving average"  彼此都有相依性。每一秒取的 "moving average" 都是有個別意義的。所以這裡沒有寫出一個具體的數字，直接看圖表的意義會比較準確。）

![](../../pictures/partitioner_experiment3_1.png)

## 結論

在高壓的環境下，使用 Strict Cost Dispatcher 時，使用 interdependent message 

* 吞吐增加 (約11%)
* 平均延遲降低 (moving average 難以量化比較，故只以圖表呈現)

## 相關

[資源充足下使用 interdependent 實驗](StrictCostDispatcher_2.md)
