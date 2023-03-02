# Kafka Q&A

這邊會整理Kafka使用上可能會遇到的問題，以及解決方法
1. [單一partition的副本同步速度太慢](#單一partition的副本同步速度太慢)
2. [Consumer poll速度太慢](#consumer-poll速度太慢)

## 單一partition的副本同步速度太慢

### 原因
Kafka端處理fetch request時，會有一個迴圈，這個迴圈會在跟os的receive buffer要資料之後還會對這些資料做一些處理，因此每次跟receive buffer poll資料會有一定的時間間隔，而receive buffer預設值是64KB, 若是調大這個buffer size可以讓buffer更慢一點塞滿，也就是說可以讓buffer能放的資料量變更多，塞滿buffer size的時間更長，更加接近每次poll資料的間隔，從而讓fetch的throughput提升
### 解法
調整Broker config中的"replica.socket.receive.buffer.bytes"，建議設定為-1，讓OS來決定buffer size大小
### 詳細討論
[#1518](https://github.com/skiptests/astraea/issues/1516)


## Consumer poll速度太慢
### 原因
consumer的socket buffer size開太小，預設為64KB，調大buffer size後，可以增加每次socket傳輸的資料量以提昇整體效能
### 解法
調整Consumer config的"receive.buffer.bytes"，建議設定為-1，讓OS來決定buffer size大小
### 詳細討論
[#1518](https://github.com/skiptests/astraea/issues/1516)

