# Astraea Balancer

Kafka 的負載(Partition)本身依照較為簡單的邏輯分配在叢集內的各個節點，這個簡單的邏輯沒有顧及到許多維護上應該注意的資源分配議題
(節點流量, 儲存空間消耗, 時間週期行為...)，對此許多 Kafka 叢集會遭遇到一些日常維護的問題，比如訊息傳遞延遲在尖峰時變高、
傳輸吞吐量遭遇瓶頸、Partition 因儲存空間用盡而下線、或是叢集資源利用率不高。

Astraea Balancer 是一個 Kafka 節點端的負載優化框架，其透過使用者自定的優化目標來重新分配 Kafka 叢集內的負載(Partition)位置，
來確保叢集的資源使用表現符合特定的預期或盡可能遠離某些風險。

* Astraea Balancer 使用文件
  * [WebService](../web_server/web_api_balancer_chinese.md)
  * [GUI](../gui/balancer/README.md)
* Astraea Balancer 實驗報告
  * [實驗報告#1](experiment_1.md)
  * [實驗報告#2](experiment_2.md)

## 成本估計

kafka partition的搬移過程中會產生一些成本，這些成本有可能會是搬移資料量,搬移時間,partition與leader需要重新連線或是佔用硬體儲存空間等資源，當叢集的規模方常大的時候，這個搬移的成本會更加的明顯，因此在搬移前需要先估計出搬移partition過程中可能產生的成本，以及對其成本做限制以確保不會佔用太多硬體資源

* 成本估計實驗報告
  * [磁碟空間限制實驗](experiment_brokerDiskSpace.md)

