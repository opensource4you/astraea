# Astraea
a collection of tools used to balance Kafka data

# Authors
- Chia-Ping Tsai <chia7712@gmail.com>
- Yi-Chen   Wang <warren215215@gmail.com>
- Ching-Hong Fang <fjh7777@gmail.com>
- Zheng-Xian Li <garyparrottt@gmail.com>
- SUN,XIANG-JUN sean0651101@gmail.com

# Quickstart a Kafka Env

There are two scripts which can setup env quickly by container

## Set up zookeeper with default version

```shell
./docker/start_zookeeper.sh
```

The above script creates a zookeeper instance by container. Also, it will show the command used to add broker instance. For example:

```shell
=================================================
run ./docker/start_broker.sh zookeeper.connect=192.168.50.178:17228 to join kafka broker
=================================================
```

## Set up zookeeper with specific version

```shell
ZOOKEEPER_VERSION=3.6.3 ./docker/start_zookeeper.sh
```

## Set up (kafka) broker with default version

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

Noted that the command to set up broker can be executed multiple times to create a broker cluster.

## Set up (kafka) broker with specific version

```shell
KAFKA_VERSION=2.8.0 ./docker/start_broker.sh zookeeper.connect=192.168.50.178:17228
```

# Kafka Tools

This project offers many kafka tools to simplify the life for kafka users.

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
1. --bootstrap.servers: the brokers addresses
2. --consumers: the number of consumers (threads). Default: 1
3. --producers: the number of producers (threads). Default: 1
4. --valueSize: the size of record value. Default: 100 bytes
5. --duration: the duration to run this benchmark. Default: 5 seconds
6. --flushDuration: the duration to flush producer records. Default: 2 seconds
