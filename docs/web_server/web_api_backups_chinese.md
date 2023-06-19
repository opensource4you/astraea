/backups
===

此 api 提供建立、查詢、刪除及修改以下備份工具：
1. `exporter`：將資料備份至指定檔案系統。
2. `importer`：從檔案系統中還原備份資料。


- [建立 exporter](#建立-exporter)
- [建立 importer](#建立-importer)
- [查詢所有 exporters 及 importer](#查詢所有-exporters-及-importers)
- [查詢指定名稱的 exporter 或 importer](#查詢指定名稱的-exporter-或-importer)
- [更改已存在的 exporter 及 importer 參數](#更改已存在的-exporter-及-importer-參數)
- [刪除 exporter 或 importer](#刪除-exporter-或-importer)

## 建立 exporter
```shell
POST /backups
```

#### exporter 參數
| 參數名稱         | 說明                                                                                                                                                                              | 預設值   |
|:-------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------|
| name         | (必填) exporter 名稱                                                                                                                                                                | 無     |
| fsSchema     | (必填) 決定儲存目標為何種檔案系統，例如: `local`, `ftp`等                                                                                                                                          | 無     |
| hostname     | (`fsSchema` 為 `ftp`, `hdfs`) 填入目標檔案系統之 `hostname`                                                                                                                               | 無     |
| port         | (`fsSchema` 為 `ftp`, `hdfs`) 填入目標檔案系統之 `port`                                                                                                                                   | 無     |
| user         | (`fsSchema` 為 `ftp`, `hdfs`) 填入目標檔案系統之登入 `user`                                                                                                                                 | 無     |
| password     | (`fsSchema` 為 `ftp`, `hdfs`) 填入目標檔案系統之登入 `password`                                                                                                                             | 無     |
| path         | (必填) 填入目標檔案系統要儲存的資料夾目錄之目標位置                                                                                                                                                     | 無     |
| topics       | (必填) 填入目標 `topics`                                                                                                                                                              | 無     |
| tasksMax     | (選填) 設定 task 數量上限                                                                                                                                                               | 1     |
| size         | (選填) 寫入檔案目標超過此設定之大小上限時會創建新檔案，並且寫入目標改為新創建之檔案。  <br/>檔案大小單位: `Bit`, `Kb`, `KiB`, `Mb`, etc.                                                                                       | 100MB |
| rollDuration | (選填) 如果在超過此時間沒有任何資料流入，會把當下所有已創建之檔案關閉，並在之後有新資料時會創建新檔案並寫入。  <br/>時間單位: `s`, `m`, `h`, `day`, etc.                                                                                 | 3s    |
| offsetFrom   | (選填）Map 格式："`topic`.`partition`.offset.from": "`offset`"<br/>針對想要的 topic 或是 topicPartition 指定備份 offset 的起點，如果要針對整個 topic 指定需將 partition 留空。ex: {"topicName.offset.from": "101"} | 無     |

cURL 範例

建立一個 exporter，將 "chia01" `topic` 的資料從 offset:101 開始備份至 local 檔案系統的 `/backup` 目錄下。
```shell
curl -X POST http://localhost:8001/backups \
    -H "Content-Type: application/json" \
    -d '{"exporter":[{
    "name": "export_local", 
    "topics": "chia01",
    "fsSchema": "local",
    "tasksMax": "3",
    "path": "/backup",
    "offsetFrom": {
      "chia01.offset.from": "101"
    }}]}' 
```

## 建立 importer
```shell
POST /backups
```
#### importer 參數

| 參數名稱              | 說明                                                                                                           | 預設值 |
|:------------------|--------------------------------------------------------------------------------------------------------------|-----|
| name              | (必填) importer 名稱                                                                                             | 無   |
| fsSchema          | (必填) 決定儲存目標為何種檔案系統，例如: `local`, `ftp`, `hdfs`等                                                               | 無   |
| hostname          | (`fsSchema` 為 `ftp`, `hdfs`) 填入目標檔案系統之 `hostname`                                                            | 無   |
| port              | (`fsSchema` 為 `ftp`, `hdfs`) 填入目標檔案系統之 `port`                                                                | 無   |
| user              | (`fsSchema` 為 `ftp`, `hdfs`)  填入目標檔案系統之登入 `user`                                                             | 無   |
| password          | (`fsSchema` 為 `ftp`, `hdfs`)  填入目標檔案系統之登入 `password`                                                         | 無   |
| path              | (必填) 填入目標檔案系統要讀取的檔案目錄位置                                                                                      | 無   |
| tasksMax          | (選填) 設定 task 數量上限                                                                                            | 1   |
| cleanSourcePolicy | (選填) 選擇已讀入之檔案的處理方式<br/>`off`：不做處理<br/>`delete`：將檔案移除<br/>`archive`： 將檔案移至`archive.dir`(須填入 `archive.dir` 參數) | off |
| archiveDir        | (`cleanSourcePolicy` 為 `archive`) 填入封存已經處理好的檔案目錄位置                                                           | 無   |

cURL 範例

建立一個 importer，從 local 檔案系統 `/backup` 目錄下讀取已備份的檔案，在讀取後將檔案移動至 `/finished` 目錄中。
```shell
curl -X POST http://localhost:8001/backups \
    -H "Content-Type: application/json" \
    -d '{"importer":[{
    "name": "import_local", 
    "fsSchema": "local",
    "tasksMax": "3",
    "path": "/backup",
    "cleanSourcePolicy": "archive",
    "archiveDir": "/finished"
    }]}' 
```

## 查詢所有 exporters 及 importers
```shell
Get /backups
```

cURL 範例

```shell
curl -X GET http://localhost:8001/backups
```

JSON Response 範例
- `name`: importer / exporter 名稱
- `configs`: importer / exporter 參數
- `state`: importer / exporter 狀態
- `tasks`: 各個 task 狀態

```json
{
    "importers": [
        {
            "name": "import_local",
            "configs": {
                "connector.class": "org.astraea.connector.backup.Importer",
                "fs.schema": "local",
                "name": "import_local",
                "path": "/backup",
                "tasks.max": "3",
                "clean.source": "archive",
                "archive.dir": "/finished",
                "header.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
                "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
                "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter"
            },
            "state": "RUNNING",
            "tasks": {
                "0": "RUNNING",
                "1": "RUNNING",
                "2": "RUNNING"
            }
        }
    ]
}
```

## 查詢指定名稱的 exporter 或 importer
```shell
Get /backups/{name}
```

cURL 範例

```shell
curl -X GET http://localhost:8001/backups/import_local
```

## 更改已存在的 exporter 及 importer 參數
```shell
PUT /backups
```

參數設置方式與[建立時的參數](#importer-參數)相同

cURL 範例

更改一個存在的 exporter，將 "chia02" `topic` 的資料備份至 local 檔案系統的 `/newBackup` 目錄下。
```shell
curl -X PUT http://localhost:8001/backups \
    -H "Content-Type: application/json" \
    -d '{"exporter":[{
    "name": "export_local", 
    "topics": "chia02",
    "fsSchema": "local",
    "tasksMax": "3",
    "path": "/newBackup"
    }]}' 
```

## 刪除 exporter 或 importer
```shell
DELETE /backups/{name}
```

cURL 範例

```shell
curl -X DELETE http://localhost:8001/backups/export_local
```