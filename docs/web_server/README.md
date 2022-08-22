Astraea Web Server 中文文件
===

Astraea 建立了一套 Web Server 服務，使用者可以透過簡易好上手的 [Web APIs](#Web-APIs) 來操作 Kafka 各項指令

## 如何啟動 Web Server ?

```shell
./docker/start_kafka_tool.sh web --bootstrap.servers 192.168.50.5:9092 --port 12345
```

- `--bootstrap.servers`: broker 位址與連接埠，用以索取叢集資訊，可填寫多台 brokers，並以 `,` 做區隔
- `--port`: web server 連接埠，此參數為選填值，不填寫的話會選一個連接埠隨機綁定
- `--jmx.port`: 目標叢集各節點所使用的JMX port，這個值指定後就可以使用 `beans` APIs

## Web APIs
- [/topics](./web_api_topics_chinese.md)
- [/groups](./web_api_groups_chinese.md)
- [/brokers](./web_api_brokers_chinese.md)
- [/producers](./web_api_producers_chinese.md)
- [/quotas](./web_api_quotas_chinese.md)
- [/transactions](./web_api_transactions_chinese.md)
- [/pipelines](./web_api_pipelines_chinese.md)
- [/beans](./web_api_beans_chinese.md)
- [/reassignments](./web_api_reassignments_chinese.md)
- [/records](./web_api_records_chinese.md)
- [/balancer](./web_api_balancer_chinese.md)
