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
define the release version of kafka. Or you can define `KAFKA_REVISION` to run kafka based on specify revision of source code.

---

## Latency Benchmark

This tool is used to test following latencies.
1. producer latency: the time of completing producer data request
2. E2E latency: the time for a record to travel through Kafka

Run the benchmark from source code
```shell
./gradlew run --args="Latency --bootstrap.servers 192.168.50.224:18878"
```

Run the benchmark from release
```shell
./bin/App Latency --bootstrap.servers 192.168.50.224:18878
```

### Latency Benchmark Configurations
1. --bootstrap.servers: the server to connect to
2. --consumers: the number of consumers (threads). Default: 1
3. --producers: the number of producers (threads). Default: 1
4. --valueSize: the size of record value. Default: 100 bytes
5. --duration: the duration to run this benchmark. Default: 5 seconds
6. --flushDuration: the duration to flush producer records. Default: 2 seconds

---

## Offset Explorer

This tool can expose both earliest offset and latest offset for all (public and private) topics.

Run the tool from source code
```shell
./gradlew run --args="offset --bootstrap.servers 192.168.50.178:19993"
```

Run the benchmark from release
```shell
./bin/App offset --bootstrap.servers 192.168.50.178:19993
```

### Offset Explorer Configurations
1. --bootstrap.servers: the server to connect to
2. --topic: the topic to search

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