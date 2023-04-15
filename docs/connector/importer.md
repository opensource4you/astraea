### Importer

此工具之目的是透過 Connector 將資料從檔案中匯入至 Kafka，輸入之資料會依照原始資料的 `topic` 及 `partition` 做匯入。

#### 支援的檔案系統
- local
- ftp
- hdfs

#### Importer Configurations

| 參數名稱            | 說明                                                                                                           | 預設值 |
|:----------------|--------------------------------------------------------------------------------------------------------------|-----|
| name            | (必填) connector 名稱                                                                                            | 無   |
| connector.class | (必填) "Importer"                                                                                              | 無   |
| fs.schema       | (必填) 決定儲存目標為何種檔案系統，例如: `local`, `ftp`, `hdfs`等                                                               | 無   |
| tasks.max       | (選填) 設定 task 數量上限                                                                                            | 1   |
| path            | (選填) 填入目標檔案系統要讀取的檔案目錄位置                                                                                      | 無   |
| clean.source    | (選填) 選擇已讀入之檔案的處理方式<br/>`off`：不做處理<br/>`delete`：將檔案移除<br/>`archive`： 將檔案移至`archive.dir`(須填入 `archive.dir` 參數) | off |
| archive.dir     | (選填) 封存已經處理好的檔案目錄位置                                                                                          | 無   |

若 `fs.schema` 為 `local` 之外的檔案系統（_{file System}_）須設定以下參數

| 參數名稱                        | 說明                           | 預設值 |
|:----------------------------|------------------------------|-----|
| fs._{file System}_.hostname | (必填) 填入目標檔案系統之 `hostname`    | 無   |
| fs._{file System}_.port     | (必填) 填入目標檔案系統之 `port`        | 無   |
| fs._{file System}_.user     | (必填)  填入目標檔案系統之登入 `user`     | 無   |
| fs._{file System}_.password | (必填)  填入目標檔案系統之登入 `password` | 無   |

#### 使用範例

```bash
# 在 worker 中創建 Importer 把 ftp server 中的資料讀入 Kafka 之中。
curl -X POST http://localhost:13575/connectors \
     -H "Content-Type: application/json" \
     -d { 
            "name": "ftp-connector", 
            "config": {
                "connector.class": "Importer",
                "fs.schema": "ftp",
                "tasks.max": "5",
                "path": "/readFromHere",
                "clean.source": "archive",
                "archive.dir": "/archiveToHere"
                "fs.ftp.hostname": "localhost",
                "fs.ftp.port": "21",
                "fs.ftp.user": "admin",
                "fs.ftp.password": "admin",
            }
        }
```