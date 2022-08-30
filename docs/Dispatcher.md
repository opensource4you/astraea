## Dispatcher

使用Astraea Dispatcher替换掉Kafka Partitioner module，使其能將發送的每筆資料按照節點負載狀況進行合理分配，達到發送端的負載平衡。

---

### SmoothDispatcher

#### SmoothDispatcher爲了解決什麼問題
分佈式流處理系統Kafka，因其容器在節點中分佈不均勻導致的叢集節點間負載不一致。負載不平衡的情況不僅會導致叢集中某些節點的硬體資源閒置，高負載節點處理資料的積壓會讓整個叢集處理資料的頻寬受制於它。我們提出SmoothDispatch ,這是一個基於叢集狀況監測，通過對收集的數據處理計算每臺節點的負載分數，以此來安排規劃每臺節點應承擔的資料負載量。當叢集中有節點達到負載上限時，Smooth Dispatch無須讓正在運行的業務下線，也能夠在負載不均勻的叢集中提升吞吐量效能，並且叢集負載越不均勻改善的程度越明顯。

#### SmoothDispatcher的特點
1. 綜合考量多維度Kafka metrics，得出能夠切實代表節點狀況的負載分數。
2. 給予高負載節點低權重，將資料按節點權重均勻分配，緩解高負載節點負載的同時降低。
3. 更平滑的資料發佈模式，高權重的節點也不會被連續選中變爲hotspot。

#### SmoothDispatcher需要配置文檔信息
在部署SmoothDispatcher，需要設置一些參數，這是因爲監測Kafka叢集狀況的數據來源於Kafka metrics。我們需要通過曝露的jmx.port來獲取這些信息。
這些需要配置的信息被記錄在了config/partitionerConfig.properties中。

#### SmoothDispatcher Configurations
通過配置partitioner參數,指定Kafka運行SmoothDispatch。
```java
class demo{
    void initConfig(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, org.astraea.partitioner.smooth.SmoothWeightRoundRobinDispatcher);
    }
}
```

#### 配置jmx port
使用Dispatcher需要你配置kafka叢集各節點的jmx port。有兩種方法進行配置。

1. 通過configs參數傳入dispatch配置文檔的地址。
```bash
--configs partitioner.config=~/astraea/config/smoothDispatchConfig.properties
```

如下是配置文檔內容，修改對應的參數即可。

```bash
#The properties are used to configure SmoothWeightPartitioner
#
# If broker.id.jmx.port is configured, then the jmx port of these brokers
# will be configured first. If there are brokers that are not configured
# to jmx port, then jmx.port will be used for configuration. And if any
# broker is not configured, and jmx.port is not configured at the same
# time, then an error will be reported.
#
###########################Smooth Partitioner Basics############################

#If no jmx port is specified for the broker,jmx.port will be used.
jmx.port=Default;

# list of brokers used for jmx port about the rest of the cluster
# format: broker.{brokerID}.jmx.port=jmx.port
#broker.0.jmx.port=9999
#broker.1.jmx.port=8888
#......
bootstrap.servers=localhost:9092
broker.id.jmx.port=Default;
```
2. 通過configs傳入下列參數
```bash
--configs bootstrap.servers=localhost:9092, jmx.port=localhost:8000
```

|            參數名稱             | 說明                                                                      |
|:---------------------------:|:------------------------------------------------------------------------|
|      bootstrap.servers      | (必填) 欲連接的Kafka server address                                           |
|          jmx.port           | (選填) 當你的jmx port擁有統一的port，那麼可以使用該參數進行一次性配置。你無需額外配置broker.id.jmx.port。   |
|     broker.id.jmx.port      | (選填) 當每一臺節點的jmx port不一致，那麼可以根據broker id進行逐一配置。沒有被配置的broker會使用jmx.port值。 |