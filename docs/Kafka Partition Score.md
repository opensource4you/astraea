### Kafka Partition Score

此工具會為broker上的partition評分，越高的分數代表負載越重

#### Start scoring partitions on broker address "192.168.103.39:9092"

```bash
./gradlew run --args="score --bootstrap.servers 192.168.103.39:9092"
```

#### Partition Score Configurations

1. --bootstrap.servers: 欲連接的server
2. --exclude.internal.topics: 若在評分時不想為內部topic評分，如:_consumer_offsets，可設置True
3. --hide.balanced: 若想隱藏已經平衡的topics、partitions，可設置True