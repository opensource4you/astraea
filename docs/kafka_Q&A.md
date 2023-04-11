# Kafka Q&A

此文件整理 Kafka 使用上可能會遇到的問題，以及解決方法

1. [單一partition的fetch速度太慢](#單一partition的fetch速度太慢)
2. [資料流入較慢的 partitions 會拖慢同節點內其他 partitions 被消費的速度](#資料流入較慢的-partitions-會拖慢同節點內其他-partitions-被消費的速度)

## 單一partition的fetch速度太慢

### 原因

Kafka端或Consumer端處理fetch request時，會有一個迴圈，這個迴圈會在跟os的receive buffer要資料之後還會對這些資料做一些處理，因此每次跟receive buffer poll資料會有一定的時間間隔，而receive buffer預設值是64KB, 若是調大這個buffer size可以讓buffer更慢一點塞滿，也就是說可以讓buffer能放的資料量變更多，塞滿buffer size的時間更長，更加接近每次poll資料的間隔，從而讓fetch的throughput提升

### 解法

Replica: 調整Broker config中的"replica.socket.receive.buffer.bytes"，建議設定為-1，讓OS來決定buffer size大小

Consumer: 調整Consumer config的"receive.buffer.bytes"，建議設定為-1，讓OS來決定buffer size大小

### 副作用

理論上把調大buffer size可以增加throughput沒錯，但是要注意的是，當調大到一定的大小之後，kafka fetch的這端可能會處理的速度比buffer進來的速度還慢，當這個現象發生時，再調大buffer size只會佔用更多的記憶體，而沒辦法再提昇fetch的效能

### 詳細討論

[#1518](https://github.com/skiptests/astraea/issues/1516)



## 資料流入較慢的 partitions 會拖慢同節點內其他 partitions 被消費的速度

### 原因

節點處理 fetch 請求時，若讀取後發現資料量不足 ，節點會延後處理此次 fetch 請求，而節點延後處理的期間，consumer 不會發送 fetch 請求給節點，導致拖慢了其他 partitions 被消費的速度

Consumer 拉取資料的簡短流程：

1. Consumer 發送 fetch 請求給各節點，若節點內有未回覆該 consumer 的請求，就不會對該節點發送 fetch 請求
2. 收到 fetch 請求的節點讀取 partitions 的資料，並判斷讀取出來的資料量有無低於 `fetch.min.bytes` ，若低於則節點會將此 fetch 請求延遲回覆

所以若有資料流入速度較慢的 partition，節點在第一時間讀取時的資料量可能無法滿足 `fetch.min.bytes` 所設定的大小，導致 consumer 的 fetch 請求被延遲回覆，影響到消費的吞吐量

### 解法

將 Consumer 端的 `fetch.max.wait.ms` 參數調小，調小後可以縮短節點延後處理 fetch 請求的時間， consumer 就能夠再對該節點發送 fetch 請求，緩解拖慢消費的情形

### 副作用

因為 consumer 發送 fetch 請求的頻率上升，broker 端所承受的請求數上升，可能會影響到 broker 其他服務的頻寬

### 詳細討論

[#1475](https://github.com/skiptests/astraea/issues/1475)

