### Topic Explorer

此工具可以顯示所有topics的 earliest offset, consumer group offset 跟 latest offset

利用source code執行此tool

```bash
./gradlew run --args="offset --bootstrap.servers 192.168.50.178:19993"
```

利用release執行此tool

```bash
java -jar app-0.0.1-SNAPSHOT-all.jar offset --bootstrap.servers 192.168.50.178:19993
```

#### Offset Explorer Configurations

1. --bootstrap.servers: 欲連接的server
2. --topics: 欲查找的topics
3. --admin.props.file: Kafka admin的properties檔案路徑