### Run Zookeeper

```bash
./docker/start_zookeeper.sh
```

此腳本透過Docker啟動Zookeeper，當成功啟動Zookeeper，腳本會輸出下列命令:

```bash
=================================================
run /home/username/Documents/astraea/docker/start_broker.sh zookeeper.connect=192.168.103.24:18098 to join kafka broker
run env CONFLUENT_BROKER=true /home/username/Documents/astraea/docker/start_broker.sh zookeeper.connect=192.168.103.24:18098 to join confluent kafka broker
=================================================
```

在執行Zookeeper腳本前，可以根據使用者需求新增環境變數，修改Zookeeper的預設配置。舉例來說，若想要更改Zookeeper的預設JVM Heap size，可以使用:

```bash
env HEAP_OPTS="-Xmx2G -Xms1G" ./docker/start_zookeeper.sh
```

