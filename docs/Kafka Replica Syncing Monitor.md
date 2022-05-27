### Kafka Replica Syncing Monitor

此工具可以監控follower replicas的同步進度，所以也可以拿來監控目前的搬移進度

#### Start monitor syncing progress

```bash
$ ./gradlew run --args="monitor --bootstrap.servers 192.168.103.39:9092"

[2021-11-23T16:00:26.282676667]
  Topic "my-topic":
  | Partition 0:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [                    ]   1.37% 0.00 B/s (unknown) []
  | Partition 1:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [                    ]   1.35% 0.00 B/s (unknown) []

[2021-11-23T16:00:26.862637796]
  Topic "my-topic":
  | Partition 0:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [#                   ]   5.62% 240.54 MB/s (11s estimated) []
  | Partition 1:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [#                   ]   5.25% 242.53 MB/s (12s estimated) []

[2021-11-23T16:00:27.400814839]
  Topic "my-topic":
  | Partition 0:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [##                  ]   9.90% 242.53 MB/s (10s estimated) []
  | Partition 1:
  | | replica on broker   0 => [####################] 100.00% [leader, synced]
  | | replica on broker   2 => [##                  ]   9.13% 240.54 MB/s (11s estimated) []

...
```

#### Replica Syncing Monitor Configurations

1. --bootstrap.servers: 欲連接的server
2. --interval: 查看replicas state的頻率，支援浮點數 (default: 1 second)
3. --prop.file: Kafka admin的properties檔案路徑
4. --topic: 要追蹤的topics (default: 追蹤所有 non-synced partition)
5. --track: 若所有replicas已經同步，仍持續監控，並持續監控任何non-synced replicas(if any) (default: false)