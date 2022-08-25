![alt text](./logo/opening.gif)

# Authors
- Chia-Ping Tsai <chia7712@gmail.com>
- Yi-Chen   Wang <warren215215@gmail.com>
- Ching-Hong Fang <fjh7777@gmail.com>
- Zheng-Xian Li <garyparrottt@gmail.com>
- Xiang-Jun Sun <sean0651101@gmail.com>
- Zhi-Mao Teng <zhimao.teng@gmail.com>
- Jia-Sheng Chen <haser1156@gmail.com>
- Chao-Heng Lee <chaohengstudent@gmail.com>
- Yi-Huan Lee <yi.huan.max@gmail.com>

# Kafka Tools

`Astraea` 提供各式工具來降低`Kafka`門檻以及提高`Kafka`效能

1. [快速啟動 Zookeeper ](./docs/run_zookeeper.md): 使用容器化的方式快速建立`zookeeper`服務
2. [快速啟動 Kafka ](./docs/run_kafka_broker.md): 使用容器化的方式快速建立`kafka broker`服務
3. [Performance Tool ](./docs/performance_benchmark.md): 可模擬多種使用情境來驗證`Kafka`叢集的吞吐量和延遲
4. [快速啟動 Prometheus ](./docs/run_prometheus.md):  建構`Kafka`叢集資訊收集系統
5. [快速啟動 Grafana ](./docs/run_grafana.md): 建置圖形化介面監控`kafka`叢集使用狀況
6. [Web Server](./docs/web_server/README.md): 可透過`Restful APIs`操作`Kafka`叢集
7. [Kafka replica syncing monitor](#Kafka-replica-syncing-monitor): Tracking replica syncing progress. (deprecated)

---

## Kafka Replica Syncing Monitor

This tool will track partition replica syncing progress. This tool can be used to observe 
the partition migration process.

### Start monitor syncing progress

```shell
$ ./gradlew run --args="monitor --bootstrap.servers 192.168.103.39:9092"

[2021-11-23T16:00:26.282676667]
  Topic "my-topic":
  | Partition 0:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [                    ]   1.37% 0.00 B/s (unknown) []
  | Partition 1:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [                    ]   1.35% 0.00 B/s (unknown) []

[2021-11-23T16:00:26.862637796]
  Topic "my-topic":
  | Partition 0:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [#                   ]   5.62% 240.54 MB/s (11s estimated) []
  | Partition 1:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [#                   ]   5.25% 242.53 MB/s (12s estimated) []

[2021-11-23T16:00:27.400814839]
  Topic "my-topic":
  | Partition 0:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [##                  ]   9.90% 242.53 MB/s (10s estimated) []
  | Partition 1:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [##                  ]   9.13% 240.54 MB/s (11s estimated) []

...
```

### Replica Syncing Monitor Configurations

1. --bootstrap.servers: the server to connect to
2. --interval: the frequency(time interval in second) to check replica state, support floating point value. (default: 1 second)
3. --prop.file: the path to a file that containing the properties to be passed to kafka admin.
4. --topic: topics to track (default: track all non-synced partition by default)
5. --track: keep track even if all the replicas are synced. Also attempts to discover any non-synced replicas. (default: false)
