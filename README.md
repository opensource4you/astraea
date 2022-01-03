# Astraea
a collection of tools used to balance Kafka data

# Authors
- Chia-Ping Tsai <chia7712@gmail.com>
- Yi-Chen   Wang <warren215215@gmail.com>
- Ching-Hong Fang <fjh7777@gmail.com>
- Zheng-Xian Li <garyparrottt@gmail.com>
- Xiang-Jun Sun <sean0651101@gmail.com>
- Zhi-Mao Teng <zhimao.teng@gmail.com>

# Kafka Tools

This project offers many kafka tools to simplify the life for kafka users.

1. [Kafka quick start](#kafka-cluster-quick-start): set up a true kafka cluster in one minute
2. [Kafka performance](#Performance-Benchmark): check producing/consuming performance.
3. [Kafka offset explorer](#topic-explorer): check the start/end offsets of kafka topics
5. [Kafka metric explorer](#kafka-metric-explorer): utility for accessing kafka Mbean metrics via JMX.
6. [Replica Collie](#replica-collie): move replicas from brokers to others. You can use this tool to obstruct specific brokers from hosting specific topics.
7. [Kafka partition score](#Kafka-partition-score): score all broker's partitions. 
8. [Kafka replica syncing monitor](#Kafka-replica-syncing-monitor): Tracking replica syncing progress.

[Release page](https://github.com/skiptests/astraea/releases) offers the uber jar including all tools.
```shell
java -jar astraea-0.0.1-alpha.1-all.jar [tool] [args]
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
broker address: 192.168.50.178:12747
jmx address: 192.168.50.178:10216
exporter address: 192.168.50.178:10558
broker id: 677
=================================================
```

1. `broker address` is used by kafka client code. The alias is bootstrap server.
2. `jmx address` exports the java metrics by JMX
3. `exporter address` is the address of prometheus exporter.

There are 4 useful ENVs which can change JVM/container configuration.
1. VERSION -> define the kafka version
2. REVISION -> define the revision of kafka source code. If this is defined, it will run distribution based on the source code
3. HEAP_OPTS -> define JVM memory options
4. DATA_FOLDERS -> define the host folders used by broker. You should define it if you want to keep data after terminating container

### Run Node Exporter

[Node Exporter](https://github.com/prometheus/node_exporter) is a famous utility for exporting machine metrics. It is 
recommended using node exporter in conjunction with [Prometheus](#run-prometheus) to observe the test environment state.

```shell
./docker/start_node_exporter.sh
```

```shell
[INFO] Container ID of node_exporter: d67d5d1daaaaf57792d145a8a8a5bd470207e698c8ca544f3023bdfcac914271
[INFO] node_exporter running at http://192.168.0.2:9100
```

### Run Prometheus

[Prometheus](https://github.com/prometheus/prometheus) is a famous application for monitor and gather time-series metrics. 

#### Start Prometheus

This project offers some scripts to set up Prometheus for your test environment quickly.

If you have an exporter installed on your broker(all Kafka instances created by `start_broker.sh` will have JMX exporter installed),
and [node exporter](#run-node-exporter) installed on your machine. You can follow the below instruction to observe the performance metrics
of Kafka & your machine.

For example. Assume you have two Kafka brokers, 

* the first broker has an exporter running at `192.168.0.1:10558` 
* the second broker has an exporter running at `192.168.0.2:10558`
* the first machine has node exporter running at `192.168.0.1:9100`
* the second machine has node exporter running at `192.168.0.2:9100`

You can execute the following command to create a Prometheus instance that fetches data from the above 4 exporters.

```shell
./docker/start_prometheus.sh start 192.168.0.1:10558,192.168.0.2:10558 192.168.0.1:9100,192.168.0.2:9100
```

The console will show the http address of prometheus service, also some hints for you to set up Grafana. See next [section](#run-grafana) for
further detail.

```shell
[INFO] Start existing prometheus instance
prometheus-9090
[INFO] =================================================
[INFO] config file: /tmp/prometheus-9090.yml
[INFO] prometheus address: http://192.168.0.2:9090
[INFO] command to run grafana at this host: ./docker/start_grafana.sh start
[INFO] command to add prometheus to grafana datasource: ./docker/start_grafana.sh add_prom_source <USERNAME>:<PASSWORD> Prometheus http://192.168.0.2:9090
[INFO] =================================================
```

#### Update Prometheus configuration

To update your Prometheus configuration, there are two ways.

1. Execute ``./docker/start_prometheus.sh refresh <kafka-exporter-addresses> <node-exporter-addresses>``.
2. Go edit ``/tmp/prometheus-9090.yml``, and execute ``./docker/start_prometheus.sh refresh``.

### Run Grafana

[Grafana](https://github.com/grafana/grafana) is a famous application for display system states. It is recommended to use Grafana
in conjunction with [Prometheus](#run-prometheus) to observe the test environment state.

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

## Performance Benchmark
This tool is used test to following metrics.
1. publish latency: the time of completing producer data request
2. E2E latency: the time for a record to travel through Kafka
3. input rate: average of consumer inputs in MByte per second
4. output rate: average of producer outputs in MByte per second

Run the benchmark from source
```shell
./gradlew run --args="performance --bootstrap.servers localhost:9092"
```
### Performance Benchmark Configurations
1. --bootstrap.servers: the server to connect to
2. --topic: the topic name. Default: testPerformance-{Time in millis}
3. --partitions: topic config when creating new topic. Default: 1 
4. --replicas: topic config when creating new topic. Default: 1
5. --consumers: the number of consumers (threads). Default: 1
6. --producers: the number of producers (threads). Default: 1
7. --run.until: the total number of records sent by the producers. Default: 1000records
8. --record.size: the (bound of) record size in byte. Default: 1 KiB
9. --fixed.size: the flag to let all records have the same size
10. --prop.file: the path to property file.
11. --partitioner: the partitioner to use in producers
12. --jmx.servers: the jmx server addresses of the brokers 

---

## Topic Explorer

This tool can expose both earliest offset and latest offset for all (public and private) topics.

Run the tool from source code
```shell
./gradlew run --args="offset --bootstrap.servers 192.168.50.178:19993"
```

Run the tool from release
```shell
java -jar app-0.0.1-SNAPSHOT-all.jar offset --bootstrap.servers 192.168.50.178:19993
```

### Offset Explorer Configurations
1. --bootstrap.servers: the server to connect to
2. --topics: the topics to be seeked
3. --admin.props.file: the file path containing the properties to be passed to kafka admin

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
java -jar app-0.0.1-SNAPSHOT-all.jar metrics --jmx.server 192.168.50.178:1099

# fetch any Mbean that its object name contains property "type=Memory".
java -jar app-0.0.1-SNAPSHOT-all.jar metrics --jmx.server 192.168.50.178:1099 --property type=Memory

# fetch any Mbean that belongs to "kafka.network" domain name,
# and it's object name contains two properties "request=Metadata" and "name=LocalTimeMs".
java -jar app-0.0.1-SNAPSHOT-all.jar metrics --jmx.server 192.168.50.178:1099 --domain kafka.network --property request=Metadata --property name=LocalTimeMs

# list all Mbeans' object name on specific JMX server.
java -jar app-0.0.1-SNAPSHOT-all.jar metrics --jmx.server 192.168.50.178:1099 --view-object-name-list
```

### Metric Explorer Configurations

1. --jmx.server: the address to connect to Kafka JMX remote server.
2. --domain: query Mbeans from the specific domain name (support wildcard "\*" and "?"). Default: "\*".
3. --property: query mbeans with the specific property (support wildcard "\*" and "?"). You can specify this argument multiple times. Default: [].
4. --strict-match: only Mbeans with its object name completely match the given criteria shows. Default: false.
5. --view-object-name-list: show the list view of MBeans' domain name & properties. Default: false.

---

## Replica Collie

This tool offers an effective way to migrate all replicas from specific brokers to others.

### Move all replicas from broker_0 and broker_1 to other brokers

```shell
./gradlew run --args="replica --bootstrap.servers 192.168.50.178:19993 --from 0,1"
```

### Move all replicas of topic "abc" from broker_0 to broker_1

```shell
./gradlew run --args="replica --bootstrap.servers 192.168.50.178:19993 --from 0 --to 1 --topics abc"
```

### Replica Collie Configurations
1. --bootstrap.servers: the server to connect to
2. --topics: the topics to be moved
3. --admin.props.file: the file path containing the properties to be passed to kafka admin

## Kafka Partition Score

This tool will score the partition on brokers, the higher score the heavier load.

### Start scoring partitions on broker address "192.168.103.39:9092"

```shell
./gradlew run --args="score --bootstrap.servers 192.168.103.39:9092"
```

### Partition Score Configurations

1. --bootstrap.servers: the server to connect to

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
