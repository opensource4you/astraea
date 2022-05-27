### Kafka Metric Explorer

此工具可以透過JMX來存取Kafka's MBean metrics

利用source code執行此tool

```bash
# fetch every Mbeans from specific JMX server.
./gradlew run --args="metrics --jmx.server 192.168.50.178:1099"

# fetch any Mbean that its object name contains property "type=Memory".
./gradlew run --args="metrics --jmx.server 192.168.50.178:1099 --property type=Memory"

# fetch any Mbean that belongs to "kafka.network" domain name, 
# and it's object name contains two properties "request=Metadata" and "name=LocalTimeMs".
./gradlew run --args="metrics --jmx.server 192.168.50.178:1099 --domain kafka.network --property request=Metadata --property name=LocalTimeMs"

# list all Mbeans' object name on specific JMX server.
./gradlew run --args="metrics --jmx.server 192.168.50.178:1099 --view-object-name-list"
```

利用release執行此tool

```bash
# fetch every Mbeans from specific JMX server.
java -jar app-0.0.1-SNAPSHOT-all.jar metrics --jmx.server 192.168.50.178:1099

# fetch any Mbean that its object name contains property "type=Memory".
java -jar app-0.0.1-SNAPSHOT-all.jar metrics --jmx.server 192.168.50.178:1099 --property type=Memory

# fetch any Mbean that belongs to "kafka.network" domain name,
# and it's object name contains two properties "request=Metadata" and "name=LocalTimeMs".
java -jar app-0.0.1-SNAPSHOT-all.jar metrics --jmx.server 192.168.50.178:1099 --domain kafka.network --property request=Metadata --property name=LocalTimeMs

# list all Mbeans' object name on specific JMX server.
java -jar app-0.0.1-SNAPSHOT-all.jar metrics --jmx.server 192.168.50.178:1099 --view-object-name-list
```

#### Metric Explorer Configurations

1. --jmx.server: 欲連接的Kafka JMX remote server address
2. --domain: 從指定的domain name詢問MBeans (支援wildcard "*" 跟 "?")。 預設: *
3. --property: 從指定的property中詢問MBeans (支援wildcard "*" 跟 "?")。 可以指定這個參數多次。 預設: []
4. --strict-match: 只顯示與它Object name完全相同的Mbeans。 預設: false
5. --view-object-name-list: 列出MBeans的 domain name & properties的list。 預設: false