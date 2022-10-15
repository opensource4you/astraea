### 調閱及設定 quota

`quota` 頁面提供您調閱及設定 `quota`

- [調閱 quota](#調閱-quota) `basic`
- [設定 quota](#設定-quota) `alter`

## 調閱 quota

`basic` tab 中，您可以透過上方選擇基於 `ip address` 或是 `client id` 來查詢對應的 `quotas`

![quota_ip](quota_ip.png)
![quota_client_id](quota_client_id.png)

您也可以透過搜尋欄位查詢指定 `ip` 或 `client id`

![quota_ip_search](quota_ip_search.png)
![quota_client_id_search](quota_client_id_search.png)

## 設定 quota

`alter` tab 中，提供您動態針對不同資源設下使用上限，目前支援三種資源：
1. `producer`：寫入速度
2. `connection`：連線數
3. `consumer`：讀取速度

如下圖分別針對 `producer` 和 `consumer` 設定讀寫上限

![quota_producer](quota_producer.png)
![quota_consumer](quota_consumer.png)

下圖則是針對指定 `ip` 限制它單位時間能建立的連線數量

![quota_connection](quota_connection.png)