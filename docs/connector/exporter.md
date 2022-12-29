### Exporter

工具之目的是透過 Connector 將資料從 Kafka 輸出出來至檔案來進行備份。

輸出之檔案會依照當初在叢集中的 `partition` 來進行資料夾的分隔，每個檔案之檔名為其包含之第一筆 `record` 的 `offset`。

目前支援之檔案系統有以下列出項目
- local
- ftp



#### Exporter Configurations

執行 Exporter 時所需要的參數及說明如下

| 參數名稱                      | 說明                                                                                          | 預設值   |
|:--------------------------|---------------------------------------------------------------------------------------------|-------|
| fs.schema                 | (必填) 決定儲存目標為何種檔案系統，例如: `local`, `ftp`等                                                      | 無     |
| fs.{file System}.hostname | (選填) 如果最初的 `fs.schema` 選定為非 `local` 之項目，需要填入目標 `host name`， `file System` 取決於前者之內容          | 無     |
| fs.{file System}.port     | (選填) 填入目標檔案系統之 `port`                                                                       | 無     |
| fs.{file System}.user     | (選填)  填入目標檔案系統之登入 `user`                                                                    | 無     |
| fs.{file System}.password | (選填)  填入目標檔案系統之登入 `password`                                                                | 無     |
| path                      | (選填)   填入目標檔案系統要儲存的資料夾目錄之目標位置                                                               | 無     |
| size                      | (選填)   寫入檔案目標超過此設定之大小上限時會創見新檔案，並且寫入目標改為新創建之檔案。  <br/>檔案大小之單位可以設定為 `Bit`, `Kb`, `KiB`, `Mb`等 | 100MB |     |


#### 使用範例

```shell
# 在 worker 中創建 exporter 把 kafka 中的資料寫入到 ftp server 之中，
# 每筆資料大小上限為 10MB，topic name 是 test

curl -x POST http://localhost:13575/connectors \
     -H "Content-Type: application/json" \
     -d '{"name": "FtpSink",
          "config": {
            "connector.class": "Exporter",
            "tasks.max": "3",
            "topics": "test",
            "fs.schema": "ftp",
            "fs.ftp.hostname": "localhost",
            "fs.ftp.port": "21",
            "fs.ftp.user": "user",
            "fs.ftp.password": "password",
            "size": "10MB",
            "path": "/test/10MB"
          }
        }'
```