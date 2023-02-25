# Kafka Q&A

這邊會整理Kafka使用上可能會遇到的問題，以及解決方法
- [partition fetch速度受到限制](##partition fetch速度受到限制)
  - [fetch from other Broker](###fetch from other Broker)
  - [fetch from Consumer](###fetch from Consumer)

## partition fetch速度受到限制
如何加快每個partition fetch的速度，詳細請看[replica leader follower sync throughput per partition](https://github.com/skiptests/astraea/issues/1516)
### fetch from other Broker
調整Broker config中的"socket.receive.buffer.bytes" 與 "replica.socket.receive.buffer.bytes"，建議設定為-1，讓OS來決定buffer size大小

### fetch from Consumer
調整Consumer config的"receive.buffer.bytes"，建議設定為-1，讓OS來決定buffer size大小

