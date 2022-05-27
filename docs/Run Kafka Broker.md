### Run Kafka Broker

Zookeeper成功啟動後，可以利用腳本輸出的訊息來啟動broker，建置你的Kafka Cluster

以上方成功啟動Zookeeper的例子來說，可以利用此command啟動broker:

```bash
/home/username/Documents/astraea/docker/start_broker.sh zookeeper.connect=192.168.103.24:18098
```

若成功啟動broker，腳本會印出該broker的訊息。 例如:

```bash
=================================================
broker id: 1001
broker address: 192.168.103.24:19473
jmx address: 192.168.103.24:14597
exporter address: 192.168.103.24:15663
=================================================
```

1. `broker address` : 供Client端連線使用，即bootstrap server
2. `jmx address` :  可以透過JMX來監控一些狀態
3. `exporter address` : Prometheus exporter的address

如果想要啟動`Confluent版本`的Kafka Cluster，可以將`CONFLUENT_BROKER`設成true。 例如:

```bash
env CONFLUENT_BROKER=true /home/username/Documents/astraea/docker/start_broker.sh zookeeper.connect=192.168.103.24:18098
```

若成功啟動`Confluent`版本的broker，腳本會印出該broker的訊息。

```ba
=================================================
broker id: 1001
broker address: 192.168.103.24:15230
exporter address: 192.168.103.24:18928
=================================================
```

有四個好用/常用的 ENVs，它們可以修改 JVM/container的配置

1. VERSION : 設置Kafka版本
2. REVISION : 設置Kafka source code版本，若被設置，會去執行source code版本的distribution
3. HEAP_OPTS : 設置 JVM memory options
4. DATA_FOLDERS : 選擇broker要在host端使用的資料夾，如果關閉broker的container後還會用到broker儲存的資料，就要設置此ENVs
