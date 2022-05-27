### Run Grafana

[Grafana](https://github.com/grafana/grafana)為顯示系統狀態的應用程式，也可以跟Prometheus一起使用，用來觀察測試環境的狀態

#### Start Grafana

此腳本可以快速啟動Grafana

```bash
./docker/start_grafana.sh start
```

```bash
aa8a47da91a2e0974a38690525f9148c9697f7ffc752611ef06248ffb09ef53a
[INFO] Default username/password for grafana docker image is admin/admin
[INFO] Access Grafana dashboard here:  http://192.168.0.2:3000
```

#### Add Prometheus DataSource

Grafana需要知道metrics來源，所以第一步是設置資料來源給你的Grafana

如下指令，設置Prometheus的資料來源給你剛剛建立的Grafana

```bash
./docker/start_grafana.sh add_prom_source <USERNAME>:<PASSWORD> Prometheus http://192.168.0.2:9090
```

```bash
{
  "datasource": {
    "id": 1,
    "uid": "7jbIw-Tnz",
    "orgId": 1,
    "name": "Prometheus",
    "type": "prometheus",
    "typeLogoUrl": "",
    "access": "proxy",
    "url": "http://192.168.0.2:9090",
    "password": "",
    "user": "",
    "database": "",
    "basicAuth": false,
    "basicAuthUser": "",
    "basicAuthPassword": "",
    "withCredentials": false,
    "isDefault": false,
    "jsonData": {},
    "secureJsonFields": {},
    "version": 1,
    "readOnly": false
  },
  "id": 1,
  "message": "Datasource added",
  "name": "Prometheus"
}
```
