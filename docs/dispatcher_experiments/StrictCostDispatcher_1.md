# Strict Cost Dispatcher 測試

此次實驗目的是驗證 Astraea partitioner 可以有效避免選到狀況較差的的節點，相比之下，Kafka partitioner則會被狀況較差的節點拖慢整體效能。

## 測試環境

### 硬體規格

|            | B1                                                         | B2       | B3       | B6       | C1       |
| ---------- | ---------------------------------------------------------- | -------- | -------- | -------- | -------- |
| CPU        | Intel i9-12900K 3.2G(5.2G)/30M/UHD770/125W                 | （同前） | （同前） | （同前） | （同前） |
| 主機板     | 微星 Z690 CARBON WIFI(ATX/1H1P/Intel 2.5G+Wi-Fi 6E)        | （同前） | （同前） | （同前） | （同前） |
| 記憶體     | 十銓 T-Force Vulcan 32G(16G*2) DDR5-5200 (CL40)            | （同前） | （同前） | （同前） | （同前） |
| 硬碟       | 威剛XPG SX8200Pro 2TB/M.2 2280/讀:3500M/寫:3000M/TLC/SMI控 | （同前） | （同前） | （同前） | （同前） |
| 散熱器     | NZXT Kraken Z53 24cm水冷排/2.4吋液晶冷頭/6年/厚:5.6cm      | （同前） | （同前） | （同前） | （同前） |
| 電源供應器 | 海韻 FOCUS GX-850(850W) 雙8/金牌/全模組                    | （同前） | （同前） | （同前） | （同前） |
| 網卡       | Marvell AQtion 10Gbit Network Adapter                      | （同前） | （同前） | （同前） | （同前） |

### 網路拓樸

```
                 switch(1G)  
                ┌──────────┐
    switch(10G) │      switch(10G)
   ┌─────┬─────┬┴────┐     │
   B1    B2    B3    C1    B6
```

### 軟體版本

* 作業系統：ubuntu-20.04.3-live-server-amd64
  * 硬碟一：
    * 50G /
    * (rest) (Kafka log directory)

  * 硬碟二：
    * 50G /home
    * (rest) (Kafka log directory)
* Astraea version: Pull Request #599 commit: 28e4f1555cd67c19a4fbdaa71118e89293eca6d5
* Zookeeper version: 3.7.1
* Apache Kafka version: 3.2.1
* Java version: OpenJDK 11
* Docker version: 20.10.17, build 100c701
* grafana image ID: cd0f3efb1eb1
* prometheus version: v2.32.1

| 執行軟體                 |  B1  |  B2  |  B3  |  B6  |  C1  |
| ------------------------ | :--: | :--: | :--: | :--: | :--: |
| Zookeeper                |  V   |      |      |      |      |
| Kakfa Broker             |  V   |  V   |  V   |  V   |      |
| Node Exporter            |  V   |  V   |  V   |  V   |      |
| Prometheus               |  V   |      |      |      |      |
| Grafana                  |  V   |      |      |      |      |
| Astraea Performance tool |      |      |      |      |  V   |

## 測試情境

叢集中一台broker，網路延遲較高、且網路頻寬較低的情境下，使用 [performance tool](../performance_benchmark.md) 來測試 producer 發送的延遲。

測試方式是：開一個 30 partitions 的 topic ，在 C1 上，使用 [Astraea performance tool](../performance_benchmark.md) 發送資料，觀察 [Astraea performance tool](../performance_benchmark.md) 輸出的平均發送延遲。

```bash
java -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -jar request599.jar performance --bootstrap.servers 192.168.103.185:9092,192.168.103.186:9092,192.168.103.187:9092,192.168.103.176:9092 --value.size 10KiB --producers 4 --consumers 0 --partitions 30 --run.until 30m --topic chia --report.path ./report
```

## 測試結果

StrictCostDispatcher 平均延遲：163.8416256 ms

Kafka Default Partitioner 平均延遲：227.8229692 ms

延遲降低 28.0837985% 

![Average Publish Latency](../pictures/partitioner_experiment_1_1.png)

## 結論

1. C1 -> B6 會經過 3 台交換機，按理說延遲會相較其他三台(B1~B3)高。
2. C1 -> B6 的網路頻寬只有 1Gbit/sec，發超過 1Gbit/sec 的話訊息會堆積，會讓發送延遲提高。

StrictCostPartitioner 根據節點平均延遲來分送訊息，也因此減少發送到 B6 （狀況較差節點）的訊息，使得在此情境下，平均延遲勝過 Kafka Default Partitioner。
