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
| frequency | (選填) 執行時拉出資料的間隔 | 300ms |

<br>

#### PerfSource

| 參數名稱                    | 說明                                                                 | 預設值         |
|-------------------------|--------------------------------------------------------------------|-------------|
| name                    | (必填) connector 名稱                                                  | 無           |
| connector.class         | (必填) connector 類別                                                  | 無           |
| topics                  | (必填) 指定要用來測試寫入的 topics                                             | 無           |
| tasks.max               | (選填) 設定 task 數量上限                                                  | 1           |
| throughput              | (選填) 用來限制輸入資料的每秒大小 <br/> 大小單位: MB, MiB, Kb etc.                    | 100GB       |
| key.distribution        | (選填) key的分佈，可用的分佈為：`uniform`, `zipfian`, `latest`, `fixed`         | uniform     |
| key.size.distribution   | (選填) key 欄位的大小的分佈，可用的分佈為：`uniform`, `zipfian`, `latest`, `fixed`   | fixed       |
| key.size                | (選填) 每筆record key的大小上限                                             | 50Byte      |
| value.distribution      | (選填) value的分佈， 可用的分佈為: `uniform`, `zipfian`, `latest`, `fixed`     | uniform     |
| value.size.distribution | (選填) value 欄位的大小的分佈，可用的分佈為：`uniform`, `zipfian`, `latest`, `fixed` | fixed       |
| value.size              | (選填) 每筆record value的大小上限                                           | 1KB         |
| specify.partitions      | (選填) 指定要傳送資料的 topic/partitions，多個項目之間可以用逗號隔開                       | 無           |
| batch.size              | (選填) batching messages 的最長大小                                       | 1           |
| key.table.seed          | (選填) 指定內部 record key 生成的隨機種子                                       | random long |
| value.table.seed        | (選填) 指定內部 record value 生成的隨機種子                                     | random long |

#### 使用範例

```bash
# 在 worker 中創建 PerfSource connector 做寫入測試。
curl -X POST http://localhost:13575/connectors \
     -H "Content-Type: application/json" \
     -d '{ 
            "name": "perf-connector", 
            "config": {
                "connector.class": "PerfSource",
                "topics":"test1",
                "tasks.max":"3",
                "throughput": "10GB",
                "key.distribution": "uniform",
                "key.length": "50Byte",
                "value.distribution": "uniform",
                "value.length": "1KB",
                "specify.partitions": "0"
            }
        }'
```

