### Run Zookeeper

#### 為什麼我們需要Zookeeper？

因為Kafka是一個**分散式**的事件串流處理平台，由於是分散式的系統，需要有一個能夠儲存叢集狀態(brokers、topics、partitions)、提供協調的系統，而Kafka利用Zookeeper來實現儲存叢集狀態的工作。

Zookeeper有兩個重要的功能:

1. 推舉出Controller，以利執行replica leader的選舉、處理節點下線時leader replica的reassign、topic的建立及刪除
2. 儲存Cluster的metadata (叢集內的broker、topics、partitions、replicas)

#### 為什麼要用腳本部署？

在部署Zookeeper時，需要設置參數、創建myid文件等等，部署完畢後還需要檢查連線是否正常，對於第一次接觸分散式系統的使用者來說，需要許多時間才能建置完成。

此專案提供了一鍵部署Zookeeper的腳本，利用Docker將Zookeeper容器化，讓使用者在建置上能夠更容易、簡單，省去大量安裝環境的時間。

#### 腳本部署方式

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

因為Zookeeper會儲存叢集的狀態，所以每當啟動一台broker，broker都必須向Zookeeper去註冊，故在利用腳本部署broker的時候需要將Zookeeper的IP與port當作參數，讓broker知道該去向誰註冊。

所以在成功建置Zookeeper後，腳本會輸出部署broker的命令，後面帶上的參數`zookeeper.connect`就是Zookeeper的IP及port。

```bash
/home/username/Documents/astraea/docker/start_broker.sh zookeeper.connect=192.168.103.24:18098
```

在執行Zookeeper腳本前，可以根據使用者需求新增環境變數，修改Zookeeper的預設配置。舉例來說，若想要更改Zookeeper的預設JVM Heap size，可以使用:

```bash
env HEAP_OPTS="-Xmx2G -Xms1G" ./docker/start_zookeeper.sh
```
