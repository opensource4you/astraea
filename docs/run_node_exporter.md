### Run Node Exporter

使用Prometheus監控host上特定的metric(e.g., CPU、硬碟、記憶體......)，需要配合Exporter將host端的port暴露出來

[Node Exporter](https://github.com/prometheus/node_exporter)有此Node exporter提供的可觀察資訊

#### 腳本啟動方式

```bash
./docker/start_node_exporter.sh
```

#### 腳本印出訊息

在`http://192.168.0.2:9100`，可以看到當前node exporter所獲取的host數據

```ba
[INFO] Container ID of node_exporter: d67d5d1daaaaf57792d145a8a8a5bd470207e698c8ca544f3023bdfcac914271
[INFO] node_exporter running at http://192.168.0.2:9100
```

##### 參數設置

腳本提供port參數設置，若使用者想設置node exporter的port，可加上`env PORT="使用者指定的port"`，若無設置，則port會使用預設的9100

###### 範例指令

```ba
env PORT="10000" ./docker/start_node_exporter.sh
```

成功啟動腳本後的輸出為

```bash
[INFO] Container ID of node_exporter: 2c1b67fca5b7b816317b608b15c87f947bb3d83a4b7a9c708da725f453d06b09
[INFO] node_exporter running at http://192.168.103.24:10000
```

##### 網頁介面

當存取`http://192.168.0.2:9100`後，可以看到下圖的Node Exporter

![image-20220628230243574](pictures/node_exporter.jpg)

如下圖，點進去Metrics後，可以看到暴露的資訊(Memory、CPU......)

![image-20220628230511180](pictures/node_exporter_metric.jpg)

