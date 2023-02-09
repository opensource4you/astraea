### Exporter

工具之目的是透過 Connector 將資料從 Kafka 輸出出來至檔案來進行備份。

輸出之檔案會依照當初在叢集中的 `partition` 來進行資料夾的分隔，每個檔案之檔名為其包含之第一筆 `record` 的 `offset`。

目前支援之檔案系統有以下列出項目

- local
- ftp

#### Exporter Configurations

執行 Exporter 時所需要的參數及說明如下

| 參數名稱                      | 說明                                                                                                           | 預設值   |
|:--------------------------|--------------------------------------------------------------------------------------------------------------|-------|
| fs.schema                 | (必填) 決定儲存目標為何種檔案系統，例如: `local`, `ftp`等                                                                       | 無     |
| path                      | (必填) 填入目標檔案系統要儲存的資料夾目錄之目標位置                                                                                  | 無     |
| fs.{file System}.hostname | (選填) 如果最初的 `fs.schema` 選定為非 `local` 之項目，需要填入目標 `host name`， `file System` 取決於前者之內容                           | 無     |
| fs.{file System}.port     | (選填) 填入目標檔案系統之 `port`                                                                                        | 無     |
| fs.{file System}.user     | (選填) 填入目標檔案系統之登入 `user`                                                                                      | 無     |
| fs.{file System}.password | (選填) 填入目標檔案系統之登入 `password`                                                                                  | 無     |
| size                      | (選填) 寫入檔案目標超過此設定之大小上限時會創見新檔案，並且寫入目標改為新創建之檔案。  <br/>檔案大小單位: `Bit`, `Kb`, `KiB`, `Mb`, etc.                    | 100MB |
| roll.duration             | (選填) 如果 `connector` 在超過此時間沒有任何資料流入，會把當下所有已創建之檔案關閉，並在之後有新資料時會創建新檔案並寫入。  <br/>時間單位: `s`, `m`, `h`, `day`, etc. | 3s    |


##### 注意
如果`worker`預設之`converter`並非為`byte array converter`
時，需要在將以下參數使設定為`org.apache.kafka.connect.converters.ByteArrayConverter`使本工具順利執行。
- key.converter
- value.converter
- header.converter

因為當`topic`內的資料為非 `worker converter`包裝的話，沒有設定`converter`會出現類似以下的錯誤資訊
```
org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler
at org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator.execAndHandleError(RetryWithToleranceOperator.java:223)
...
Caused by: org.apache.kafka.connect.errors.DataException: Converting byte[] to Kafka Connect data failed due to serialization error:
at org.apache.kafka.connect.json.JsonConverter.toConnectData(JsonConverter.java:324)
...
Caused by: org.apache.kafka.common.errors.SerializationException: com.fasterxml.jackson.core.JsonParseException: Unrecognized token 'test': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')
...
Caused by: com.fasterxml.jackson.core.JsonParseException: Unrecognized token 'test': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')
...
```



#### 使用範例

```shell
# 在 worker 中創建 exporter 把 kafka 中的資料寫入到 ftp server 之中，
# 每筆資料大小上限為 10MB，topic name 是 test，
# 並且如果超過3秒沒有新資料流入，會結束所有正在寫入之檔案，等有新資料流入時會創建新的檔案來寫入

curl -X POST http://localhost:13575/connectors \
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
            "roll.duration": "3s"
            "path": "/test/10MB",
          }
        }'
```