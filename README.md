# Astraea
a collection of tools used to balance Kafka data

# Authors
- Chia-Ping Tsai <chia7712@gmail.com>
- Yi-Chen   Wang <warren215215@gmail.com>
- Ching-Hong Fang <fjh7777@gmail.com>
- Zheng-Xian Li <garyparrottt@gmail.com>

# Quickstart

There are two scripts which can setup env quickly by container

## Set up zookeeper env

```shell
./docker/start_zk.sh
```

The above script creates a zookeeper instance by container. Also, it will show the command used to add broker instance. For example:

```shell
9ad8f73ca9303956fb1dc17e48bfcba7c4876d68b1a2ff6056b560ff1c88a8c9
=================================================
run ./docker/start_broker.sh zookeeper.connect=192.168.50.178:17228 to join kafka broker
=================================================
```

## Set up (kafka) broker env

After the zk env is running, you can copy the command (see above example) from zk script output to setup kafka. For example:
```shell
./docker/start_broker.sh zookeeper.connect=192.168.50.178:17228
```

The console will show the broker connection information and JMX address. For example:

```shell
873f799266e27a85f51aca7e0f64bc159a024354a8c742473d62e50a2f3b78fd
=================================================
broker address 192.168.50.224:11248
jmx address: 192.168.50.224:15905
=================================================
```

Noted that the command to set up broker can be executed multiples to create a broker cluster.