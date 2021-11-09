# Astraea
a collection of tools used to balance Kafka data

# Authors
- Chia-Ping Tsai <chia7712@gmail.com>
- Yi-Chen   Wang <warren215215@gmail.com>
- Ching-Hong Fang <fjh7777@gmail.com>
- Zheng-Xian Li <garyparrottt@gmail.com>
- Xiang-Jun Sun <sean0651101@gmail.com>

# Kafka Tools

This project offers many kafka tools to simplify the life for kafka users.

1. [Kafka quick start](#kafka-cluster-quick-start): set up a true kafka cluster in one minute
2. [Kafka performance](#Performance-Benchmark): check producing/consuming performance.
3. [Kafka offset explorer](#topic-explorer): check the start/end offsets of kafka topics
4. [Kafka official tool](#kafka-official-tool): run any one specific kafka official tool. All you have to prepare is the docker env.
5. [Kafka metric client](#kafka-metric-client): utility for accessing kafka Mbean metrics via JMX.
6. [Replica Collie](#replica-collie): move replicas from brokers to others. You can use this tool to obstruct specific brokers from hosting specific topics.

[Release page](https://github.com/skiptests/astraea/releases) offers the uber jar including all tools.
```shell
java -jar astraea-0.0.1-alpha.1-all.jar [tool] [args]
```

---

## Kafka Cluster Quick Start

The following scripts can build a kafka cluster by containers in one minute.

### Set up zookeeper

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

### Set up (kafka) broker

After the zk env is running, you can copy the command (see above example) from zk script output to set up kafka. For example:
```shell
./docker/start_broker.sh zookeeper.connect=192.168.50.178:17228
```

The console will show the broker connection information and JMX address. For example:

```shell
=================================================
broker address: 192.168.50.224:11248
jmx address: 192.168.50.224:15905
=================================================
```

The command to set up broker can be executed multiple times to create a broker cluster. The env `KAFKA_VERSION` is used to
define the release version of kafka. Or you can define `KAFKA_REVISION` to run kafka based on specific revision of source code.

---

## Performance Benchmark
This tool is used test to following metrics.
1. publish latency: the time of completing producer data request
2. E2E latency: the time for a record to travel through Kafka
3. input rate: sum of consumer inputs in MByte per second
4. output rate: sum of producer outputs in MByte per second

Run the benchmark from source
```shell
./gradlew run --args="Performance --bootstrap.servers localhost:9092 --topic topic --topicConfig partitions:10,replicationFactor:3 --producers 5 --consumers 1 --records 100000 --recordSize 10000"
```
### Performance Benchmark Configurations
1. --bootstrap.servers: the server to connect to
2. --topic: the topic name
3. --partitions: topic config when creating new topic. Default: 1 
4. --replicas: topic config when creating new topic. Default: 1
5. --consumers: the number of consumers (threads). Default: 1
6. --records: the total number of records sent by the producers. Default: 1000
7. --record.size: the record size in byte. Default: 1024 byte

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

## Kafka Metric Client

This tool can be used to access Kafka's MBean metrics via JMX.

Run the tool from source code

```shell
./gradlew run --args="metrics --jmx.server 192.168.50.178:1099"
./gradlew run --args="metrics --jmx.server 192.168.50.178:1099 --property type=Memory"
./gradlew run --args="metrics --jmx.server 192.168.50.178:1099 --view-object-name-list"
./gradlew run --args="metrics --jmx.server 192.168.50.178:1099 --domain kafka.network --property request=Metadata --property name=LocalTimeMs"
```

Run the tool from release
```shell
java -jar app-0.0.1-SNAPSHOT-all.jar metrics --jmx.server 192.168.50.178:1099
java -jar app-0.0.1-SNAPSHOT-all.jar metrics --jmx.server 192.168.50.178:1099 --property type=Memory
java -jar app-0.0.1-SNAPSHOT-all.jar metrics --jmx.server 192.168.50.178:1099 --view-object-name-list
java -jar app-0.0.1-SNAPSHOT-all.jar metrics --jmx.server 192.168.50.178:1099 --domain kafka.network --property request=Metadata --property name=LocalTimeMs
```

### Metric Client Configurations

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