![alt text](./logo/logo_with_background.png)

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

This project offers many kafka tools to simplify the life for kafka users.

1. [Kafka quick start](#kafka-cluster-quick-start): set up a true kafka cluster in one minute
2. [Kafka performance](./docs/performance_benchmark.md): 可產生不同類型資料集測試讀寫速度及E2E延遲的工具
3. [Kafka_Prometheus](./docs/run_prometheus.md):  整合 Kafka 與 Prometheus
4. [快速啟動Grafana](./docs/run_grafana.md): 建置圖形化介面監控Kafka server、Host端資源使用量
5. [Kafka metric explorer](#kafka-metric-explorer): utility for accessing kafka Mbean metrics via JMX.
6. [Kafka replica syncing monitor](#Kafka-replica-syncing-monitor): Tracking replica syncing progress.
7. [Astraea Web Server 中文文件連結](./docs/web_server/README.md)

[Github packages](https://github.com/orgs/skiptests/packages?repo_name=astraea) offers the docker image to run mentioned tools
```shell
./docker/start_app.sh web --bootstrap.servers 192.168.50.178:19993 --port 12345"
```

---

## Kafka Cluster Quick Start

The following scripts can build a kafka cluster by containers in one minute.

### Run Zookeeper

```shell
./docker/start_zookeeper.sh
```

The script creates a zookeeper instance by container. Also, it will show the command used to add broker instance. For example:

```shell
=================================================
run ./docker/start_broker.sh zookeeper.connect=192.168.50.178:17228 to join kafka broker
=================================================
```

You can define `VERSION` to change the binary version.

### Run Kafka Broker

After the zk env is running, you can copy the command (see above example) from zk script output to set up kafka. For example:

```shell
./docker/start_broker.sh zookeeper.connect=192.168.50.178:17228
```

The console will show the broker connection information and JMX address. For example:

```shell
=================================================
broker id: 677
broker address: 192.168.50.178:12747
jmx address: 192.168.50.178:10216
exporter address: 192.168.50.178:10558
=================================================
```

1. `broker address` is used by kafka client code. The alias is bootstrap server.
2. `jmx address` exports the java metrics by JMX
3. `exporter address` is the address of prometheus exporter.

You can set `CONFLUENT_BROKER` to true, if you want start the confluent version of the kafka cluster. For example:

```shell
env CONFLUENT_BROKER=true ./docker/start_broker.sh zookeeper.connect=192.168.50.178:17228
```

The console will show the broker connection information and exporter address. For example:

```shell
=================================================
broker id: 1001
broker address: 192.168.103.39:15230
exporter address: 192.168.103.39:18928
=================================================
```

There are 4 useful ENVs which can change JVM/container configuration.

1. VERSION -> define the kafka version
2. REVISION -> define the revision of kafka source code. If this is defined, it will run distribution based on the source code
3. HEAP_OPTS -> define JVM memory options
4. DATA_FOLDERS -> define the host folders used by broker. You should define it if you want to keep data after terminating container

### Run Node Exporter

[Node Exporter](https://github.com/prometheus/node_exporter) is a famous utility for exporting machine metrics. It is 
recommended using node exporter in conjunction with Prometheus to observe the test environment state.

```shell
./docker/start_node_exporter.sh
```

```shell
[INFO] Container ID of node_exporter: d67d5d1daaaaf57792d145a8a8a5bd470207e698c8ca544f3023bdfcac914271
[INFO] node_exporter running at http://192.168.0.2:9100
```

### Run Grafana

[Grafana](https://github.com/grafana/grafana) is a famous application for display system states. It is recommended to use Grafana
in conjunction with Prometheus to observe the test environment state.

#### Start Grafana

This project offers a way to quickly create a Grafana container instance for **test purpose**.

```shell
./docker/start_grafana.sh start
```

```shell
aa8a47da91a2e0974a38690525f9148c9697f7ffc752611ef06248ffb09ef53a
[INFO] Default username/password for grafana docker image is admin/admin
[INFO] Access Grafana dashboard here:  http://192.168.0.2:3000
```

#### Add Prometheus DataSource

Grafana needs to know where the metrics are, so he can show you the pretty diagram. The first step is setting up the 
the data source for your Grafana instance.

The following command set up a Prometheus data source for the Grafana instance we previously created.

```shell
./docker/start_grafana.sh add_prom_source <USERNAME>:<PASSWORD> Prometheus http://192.168.0.2:9090
```
```json
{
  "datasource": {
    "id": 1,
    "uid": "7jbIw-Tnz",
    "orgId": 1,
    "name": "Prometheus",
    "type": "prometheus",
    "typeLogoUrl": "",
    "access": "proxy",
    "url": "http://192.168.0.2:9090",
    "password": "",
    "user": "",
    "database": "",
    "basicAuth": false,
    "basicAuthUser": "",
    "basicAuthPassword": "",
    "withCredentials": false,
    "isDefault": false,
    "jsonData": {},
    "secureJsonFields": {},
    "version": 1,
    "readOnly": false
  },
  "id": 1,
  "message": "Datasource added",
  "name": "Prometheus"
}
```

---

## Kafka Metric Explorer

This tool can be used to access Kafka's MBean metrics via JMX.

Run the tool from source code

```shell
# fetch every Mbeans from specific JMX server.
./gradlew run --args="metrics --jmx.server 192.168.50.178:1099"

# fetch any Mbean that its object name contains property "type=Memory".
./gradlew run --args="metrics --jmx.server 192.168.50.178:1099 --property type=Memory"

# fetch any Mbean that belongs to "kafka.network" domain name, 
# and it's object name contains two properties "request=Metadata" and "name=LocalTimeMs".
./gradlew run --args="metrics --jmx.server 192.168.50.178:1099 --domain kafka.network --property request=Metadata --property name=LocalTimeMs"

# list all Mbeans' object name on specific JMX server.
./gradlew run --args="metrics --jmx.server 192.168.50.178:1099 --view-object-name-list"
```

Run the tool from release
```shell
# fetch every Mbeans from specific JMX server.
./docker/start_app.sh metrics --jmx.server 192.168.50.178:1099

# fetch any Mbean that its object name contains property "type=Memory".
./docker/start_app.sh metrics --jmx.server 192.168.50.178:1099 --property type=Memory

# fetch any Mbean that belongs to "kafka.network" domain name,
# and it's object name contains two properties "request=Metadata" and "name=LocalTimeMs".
./docker/start_app.sh metrics --jmx.server 192.168.50.178:1099 --domain kafka.network --property request=Metadata --property name=LocalTimeMs

# list all Mbeans' object name on specific JMX server.
./docker/start_app.sh metrics --jmx.server 192.168.50.178:1099 --view-object-name-list
```

### Metric Explorer Configurations

1. --jmx.server: the address to connect to Kafka JMX remote server.
2. --domain: query Mbeans from the specific domain name (support wildcard "\*" and "?"). Default: "\*".
3. --property: query mbeans with the specific property (support wildcard "\*" and "?"). You can specify this argument multiple times. Default: [].
4. --strict-match: only Mbeans with its object name completely match the given criteria shows. Default: false.
5. --view-object-name-list: show the list view of MBeans' domain name & properties. Default: false.

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
