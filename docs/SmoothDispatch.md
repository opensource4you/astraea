### Run SmoothDispatch

#### SmoothDispatch爲了解決什麼問題
分佈式流處理系統Kafka，因其容器在節點中分佈不均勻導致的叢集節點間負載不一致。負載不平衡的情況不僅會導致叢集中某些節點的硬體資源閒置，高負載節點處理資料的積壓會讓整個叢集處理資料的頻寬受制於它。我們提出SmoothDispatch ,這是一個基於叢集狀況監測，通過對收集的數據處理計算每臺節點的負載分數，以此來安排規劃每臺節點應承擔的資料負載量。我們的實驗表明，當叢集中有節點達到負載上線時，Astraea Partitioner無須讓正在運行的業務下線，也能夠在負載不均勻的叢集中提升吞吐量效能，並且叢集負載越不均勻改善的程度越明顯。


#### SmoothDispatch需要配置文檔信息
在部署SmoothDispatach，需要設置一些參數，這是因爲監測Kafka叢集狀況的數據來源於Kafka metrics。我們需要通過曝露的jmx.port來獲取這些信息。
這些需要配置的信息被記錄在了config/partitionerConfig.properties中。

#### SmoothDispatch Configurations
```bash
--partitioner org.astraea.partitioner.smooth.SmoothWeightRoundRobinDispatcher
```
通過partitioner參數指定Kafka運行SmoothDispatch。

```bash
--configs partitioner.config=~/astraea/config/smoothDispatchConfig.properties
```
通過configs參數傳入dispatch配置文檔的地址。

```bash
~/astraea/config/partitionerConfig.properties
```
這是文檔的默認位置，根據文檔中的註釋正確配置jmx port，SmoothDispatch即可正常運行。