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
4. [Kafka official tool](#kafka-official-tool): run any one specific kafka official tool. All you have to prepare is the docker env.
5. [Kafka metric client](#kafka-metric-client): utility for accessing kafka Mbean metrics via JMX.
6. [Replica Collie](#replica-collie): move replicas from brokers to others. You can use this tool to obstruct specific brokers from hosting specific topics.
7. [Kafka partition score](#Kafka-partition-score): score all broker's partitions. 

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

You can define `ZOOKEEPER_VERSION` to change the binary version.

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
1. KAFKA_VERSION -> define the kafka version
2. KAFKA_REVISION -> define the revision of kafka source code. If this is defined, it will run distribution based on the source code
3. KAFKA_HEAP_OPTS -> define JVM memory options
4. LOG_FOLDERS -> define the host folders used by broker. You should define it if you want to keep data after terminating container

### Run Prometheus

The exporters of brokers can offer metrics to Prometheus to monitor cluster status. For example, your kafka cluster has
two brokers. The exporter address of first broker is `192.168.50.178:10558` and another is `192.168.50.178:10553`. You can
use following command to run a prometheus service.

```shell
./docker/start_prometheus.sh 192.168.50.178:10558,192.168.50.178:10553
```

The console will show the http address of prometheus service.
```shell
=================================================
config file: /tmp/prometheus-15483.yml
prometheus address: http://192.168.50.178:15483
=================================================
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
7. --run.until: the total number of records sent by the producers. Default: records:1000
8. --record.size: the (bound of) record size in byte. Default: 1024 byte
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

## Kafka Official Tool

This project offers a way to run kafka official tool by container. For example:

### Run kafka-topics.sh

```shell
./docker/start_kafka_tool.sh kafka-topics.sh --bootstrap-server 192.168.50.178:14082 --list
```

### Show Available Official Tools

```shell
./docker/start_kafka_tool.sh help
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

