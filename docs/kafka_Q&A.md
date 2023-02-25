# Kafka Q&A

這邊會整理Kafka使用上可能會遇到的問題，以及解決方法
1. [單一partition 的副本同步速度太慢](##單一partition的副本同步速度太慢)
2. [Consumer poll速度太慢](##Consumer poll速度太慢)

## 單一partition的副本同步速度太慢

### 原因
replica與broker的socket buffer size開太小，預設分別是64KB與100KB 
### 解法
調整Broker config中的"socket.receive.buffer.bytes" 與 "replica.socket.receive.buffer.bytes"，建議設定為-1，讓OS來決定buffer size大小
### 詳細討論
[#1518](https://github.com/skiptests/astraea/issues/1516)


## Consumer poll速度太慢
### 原因
consumer的socket buffer size開太小，預設為64KB
### 解法
調整Consumer config的"receive.buffer.bytes"，建議設定為-1，讓OS來決定buffer size大小
### 詳細討論
[#1518](https://github.com/skiptests/astraea/issues/1516)

