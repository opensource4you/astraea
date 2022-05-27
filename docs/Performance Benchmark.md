### Performance Benchmark

此工具用來測試下列的metrics:

1. Publish latency : 完成producer request的時間
2. End-to-End latency : 一筆record從producer端到consumer端的時間
3. Input rate : consumer拉取資料的速率(MB/s)
4. Output rate : producer送資料的速率(MB/s)

執行指令為:

```bash
./gradlew run --args="performance --bootstrap.servers localhost:9092"
```

#### Performance Benchmark Configurations

1. --bootstrap.servers: 欲連接的server

2. --compression: Producer使用的壓縮演算法

3. --topic: 選擇topic name。 預設: testPerformance-{Time in millis}

4. --partitions: 新增新的topic時，新增的partition數目。 預設: 1

5. --replicas: 新增新的topic時，新增的replica數目。 預設: 1

6. --consumers: 欲開啟的consumer thread(s)數量。 預設: 1

7. --producers: 欲開啟的producer thread(s)數量。 預設: 1

8. --run.until: producers要送的records數或是producers在給定時間內一直發送資料。

   若是選擇producers要送多少records，參數可以使用"--run.until 89242records"，預設: 1000records。 

   若選擇producer在給定時間內發送資料，則參數可以使用"--run.until 1m"，時間單位可以選擇"days", "day", "h", "m", "s", "ms", "us", "ns"。

9. --record.size: 每筆record的大小上限，以byte計算。 預設: 1KiB

10. --prop.file: 配置property file的路徑

11. --partitioner: 配置producer使用的partitioner

12. --configs: 給partitioner的設置檔。 設置格式為 "<key1>=<value1>[,<key2>=<value2>]*"。 例如: "--configs broker.1001.jmx.port=14338,org.astraea.cost.ThroughputCost=1"

13. --throughput: 所有producers的produce rate。 例如: "--throughput 2MiB"。 預設: 500 GiB (每秒)

14. --key.distribution: key的分佈名稱，可用的分佈為: "uniform", "zipfian", "latest", "fixed"。 預設: (No Key)

15. --size.distribution: value大小的分佈名稱， 可用的分佈為: "uniform", "zipfian", "latest", "fixed"。預設: "uniform"

16. --specify.broker: 指定broker的ID，送資料到指定的broker。 預設: (Do Not Specify)

17. --report.path: report file的檔案路徑。 預設: (No report)

18. --report.format: 選擇輸出檔案格式。 可用的格式: "csv", "json"。 預設: "csv"

19. --transaction.size: 每個transaction的records數量。 預設: 1