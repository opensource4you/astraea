### Import csv
此工具可批量對csv格式文件進行數據清理。

#### 目前支援的資料源
在配置與資料路徑相關的argument時，需要根據資料源遵守其對應的格式。
1. local

   argument example: local:/home/source
   * /home/source 目標地址

2. ftp

    argument example: ftp@user:password://0.0.0.0:8888/home/source 
    * user 用戶名
    * password 密碼
    * 0.0.0.0:8888 host:port
    * /home/source 目標地址

#### Import csv Configurations
|       參數名稱       | 說明                                                                                                                   | 預設值 |
|:----------------:|:---------------------------------------------------------------------------------------------------------------------|:---:|
|      source      | (必填) 待處理的csv資料來源                                                                                                     |  無  |
|       sink       | (必填) 處理完畢的csv資料落庫地點                                                                                                  |  無  |
|   cleanSource    | 決定以何種方式清理處理完畢的csv文件<br/>有三種模式可供選擇：<br/>1.off: 不對處理完畢的csv進行操作<br/> 2. delete:刪除處理完畢的csv<br/> 3. archive:將處理完畢的csv進行封存 | off |
| sourceArchiveDir | 當cleanSource被配置爲archive的時候就需要指定archive地點，需要注意的是archive的地址應該避免與source重複                                               |  無  |

#### 使用範例

專案內的工具都有整合到`container`中，使用者利用docker運行，即可方便管理。

使用`docker`執行`Impoet csv`

```bash 
docker/start_app.sh clean_csv --source local:/home/source --sink local:/home/sink
```

`Import csv`可以指定資料的上下游，及選擇如何清理處理完畢的資料。
以下僅列出一些使用範例：

```bash
# 待處理csv來源ip 192.0.0.1：5050，登錄用戶名爲user，密碼8888，來源路徑爲/home/source。處理完畢的資料落庫位置爲本機/home/sink。
docker/start_app.sh clean_csv --source ftp://user:8888@192.0.0.1:5050/home/source --sink local:/home/sink
```

```bash
# 待處理csv來源路徑爲/home/source。處理完畢的資料落庫位置爲本機/home/sink。處理完畢的資料會被刪除
docker/start_app.sh clean_csv --source local:/home/source --sink local:/home/sink --cleanSource delete
```

```bash
# 待處理csv來源路徑爲/home/source。處理完畢的資料落庫位置爲本機/home/sink。處理完畢的資料會被做archive操作，封存在本地的/home/archive目錄下
docker/start_app.sh clean_csv --source local:/home/source --sink local:/home/sink --cleanSource archive --sourceArchiveDir local:/home/archive
```