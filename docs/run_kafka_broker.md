### Run Kafka Broker

#### Broker介紹

Kafka Cluster由多個brokers組成，一個broker內儲存多個topics，一個topic又可以分成多個partitions。Kafka producer、consumer是向broker發送訊息、拉取訊息的client端。

#### 為什麼要用腳本部署broker？

此專案提供了一鍵部署Kafka broker的腳本，利用Docker將Kafka容器化，讓使用者在建置上能夠更容易、簡單，省去大量安裝環境的時間。
腳本內還整合了JMX以及Prometheus exporter，讓使用者可以監控Mbeans、hardware、OS的metrics。

`Kafka`叢集需要一套可管理`metadata`的服務，舊版仰賴`zookeeper` ，新版則可使用`Kafka`內建的`kraft`服務。

#### broker with zookeeper

[成功部署Zookeeper後](./run_zookeeper.md)，可以利用`Zookeeper`腳本輸出的訊息來啟動broker

```bash
/home/username/Documents/astraea/docker/start_broker.sh zookeeper.connect=192.168.103.24:18098
```

#### broker with kraft

下方指令可運行單節點的`kraft`服務：

```bash
./docker/start_controller.sh
```

`kraft`順利啟動後會顯示可用於執行`broker`服務的指令，如下所示：

```bash
run CLUSTER_ID=c3J9X3aoVA4n1zWaTG9PtD /Users/chia7712/project/astraea/docker/start_broker.sh controller.quorum.voters=13270@192.168.111.250:17340 to join broker
```

若成功啟動broker，腳本會印出該broker的訊息。 例如:

```bash
=================================================
broker address: 192.168.103.24:19473
jmx address: 192.168.103.24:14597
exporter address: 192.168.103.24:15663
=================================================
```

1. `broker address` : 供Client端連線使用，即bootstrap server
2. `jmx address` :  可以透過JMX來監控一些狀態
3. `exporter address` : Prometheus exporter的address，可用來監控hardware及OS的metrics

#### 環境變數設置

有四個好用的 ENVs，它們可以修改 JVM/container的配置，使用者可隨著自己的需求改動

1. VERSION : 設置Kafka版本，會去下載官方已經建置好的distribution
2. REVISION : 設置Kafka source code版本，會去下載原始碼並編譯建置可執行檔後部署
3. HEAP_OPTS : 設置 JVM memory options
4. DATA_FOLDERS : 選擇broker要在host端使用的資料夾，如果關閉容器後還會用到broker儲存的資料，就要設置此環境變數

##### 版本選擇

此腳本所建置的Kafka版本是先看使用者有無設置revision版本，若無設置revision版本才會去看version版本
