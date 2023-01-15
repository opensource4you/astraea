### Run Hadoop

#### Hadoop 介紹
[Apache Hadoop](https://github.com/apache/hadoop)是一個開源專案，提供可靠、可擴展的分散式計算。

Apache Hadoop software library是一個框架，允許在電腦叢集上使用簡單的模型對大數據進行分散式處理。它設計於將單個server擴展到數千台機器，每台機器都提供本地運算和存儲。本身設計為在應用層面檢測和處理故障，因此可以在可能出現故障的電腦叢集上提供高可用性服務。

#### Hadoop Distributed File System (HDFS) 介紹

`HDFS`是一個主從式架構。在`HDFS`中，檔案被分成一個或多個 blocks 並且儲存在一組 DataNode 中

- `NameNode`執行文件系統命名空間操作，如打開、關閉和重命名文件和目錄。它也負責決定 block 與 DataNode 的映射。

- `DataNode`負責從文件系統的客戶端提供讀取和寫入請求。DataNode 也會根據 NameNode 的指示執行 block 創建、刪除和複製。

##### 腳本部署方式

1. 啟動 `NameNode`
   ##### 腳本
   ```bash
   ./docker/start_namenode.sh [OPTIONS]
   ```
   `[OPTIONS]`為一或多組`hdfs-site.xml` name=value 參數，可以參考[官方docs](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml)提供的各項參數及預設值
    
   若成功啟動 NameNode，腳本會輸出下列命令：
   ##### 腳本輸出
   ```bash
   efe7d7da5fc3bb26f5efaf4314804bbb6d6b4350226603b92826b733ae1426bb
   =================================================
   jmx address: 192.168.103.44:12937/jmx
   run /home/username/IdeaProjects/astraea/docker/start_datanode.sh fs.defaultFS=hdfs://namenode-12991:8020 to join datanode
   =================================================
   ```
   可以根據輸出的 ip port (ex. `http://192.168.103.44:12937`)進入官方提供的 WebUI 介面
---
2. 啟動 `DataNode`

   成功建置 NameNode 後，腳本會輸出部署 DataNode 的命令，後面的參數`fs.defaultFS`就是 NameNode 的 hostname 及 port
   ##### 腳本
   ```bash
   ./docker/start_datanode.sh fs.defaultFS=hdfs://namenode-12991:8020 [OPTIONS]
   ```
   `[OPTIONS]`為一或多組`hdfs-site.xml` name=value 參數，可以參考[官方docs](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml)提供的各項參數及預設值

   若成功啟動 DataNode，腳本會輸出以下結果：
   ##### 腳本輸出
   ```bash
   496ff7a8d17b6002310bbfd6d730fa8817fc58d022d1bb37c7f2725126a53857
   =================================================
   jmx address: 192.168.103.44:11641/jmx
   =================================================
   ```
   同樣可以根據輸出的 ip port (ex. `http://192.168.103.44:11641`)進入官方提供的 WebUI 介面

   重複執行此腳本即可在 NameNode 下啟動多個 DataNode