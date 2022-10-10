### Run Kafka Worker

#### Worker介紹

Worker是一個基於Kafka的服務，可以更方便的創建管理數據流管道，本身為可擴展的服務且可透過connectors將不同資料來源導入到kafka，並進行資料轉換，亦可從kafka導出到其他系統。

#### 為什麼要用腳本部署worker？

部署一個Kafka worker需耗費許多時間設置參數、測試與Kafka broker通訊是否正常。

此專案提供了一鍵部署Kafka worker，利用Docker將Kafka容器化，讓使用者在建置上能夠更容易、簡單，省去大量安裝環境的時間。

部署worker前請先確認broker已經部署成功，因為需要取得broker的host與port當作啟動worker腳本的參數。

#### 一般版本的worker

成功部署Kafka broker後，可以利用broker腳本輸出的訊息來啟動worker

```bash
/home/username/Documents/astraea/docker/start_worker.sh bootstrap.servers=192.168.1.101:16072
```

若成功啟動broker，腳本會印出該broker的訊息。 例如:

```bash
=================================================
worker address: 192.168.1.101:16564
group.id: worker-uddds
=================================================
```

1. `worker address` : 供Client端連線使用
2. `group.id` : 群集id，用以判斷各個worker節點是否處於同一群集，預設每次啟動此腳本都會替worker建立一隨機group.id。使用者亦可透過腳本自行指定，例如 group.id=connect-cluster

#### Confluent版本的worker

此腳本提供啟動`Confluent版本`的Kafka worker，可以將環境變數`CONFLUENT_WORKER`設成true。若要指定 confluent 版本，可設置環境變數`CONFLUENT_VERSION`。
例如:

```bash
env CONFLUENT_WORKER=true CONFLUENT_VERSION=7.0.1 /home/username/Documents/astraea/docker/start_broker.sh bootstrap.servers=192.168.1.101:16072
```

若成功啟動`Confluent`版本的broker，腳本會印出該broker的訊息。

```bash
=================================================
connect address: 192.168.1.101:14418
group.id: connect-SuXd9
=================================================
```

#### 環境變數設置

有三個好用的 ENVs，它們可以修改 JVM/container的配置，使用者可隨著自己的需求改動

1. VERSION : 設置Kafka版本，會去下載官方已經建置好的distribution
2. REVISION : 設置Kafka source code版本，會去下載原始碼並編譯建置可執行檔後部署
3. HEAP_OPTS : 設置 JVM memory options

##### 版本選擇

此腳本所建置的Kafka版本是先看使用者有無設置revision版本，若無設置revision版本才會去看version版本

##### Worker connector plugins 路徑

所有connector plugin的jar檔案均需放置在本機端`/tmp/connectors`底下
