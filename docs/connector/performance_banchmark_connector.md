### Performance Benchmark Connector

本專案已有[Performance Benchmark](../performance_benchmark.md)工具可產生各種不同類型之資料來測試讀寫速度, E2E延遲。
此為上述工具之`Connector`版本。目的在於方便擴充使用在多機器上進行測試。

基於`Connector`設計，以下分為 `PerfSink` 與 `PerfSource` 來進行資料的輸入及輸出。

<!-- TOC -->
  * [PerfSink](#perfsink)
  * [PerfSource](#perfsource)
<!-- TOC -->


#### PerfSink
此目的是為了把資料從 Kafka 拉出來，因此需要以下參數來進行設定。

| 參數名稱      | 說明              | 預設值   |
|-----------|-----------------|-------|
| frequency | (必填) 執行時拉出資料的間隔 | 300ms |




#### PerfSource