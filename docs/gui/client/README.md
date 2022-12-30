### client

`client` 頁面提供您:
* 查詢 `consumer` `producer` `transaction` 的資訊。
* 使用`read`頁面來查看讀取數據的狀況
* 使用`write`頁面來寫入 `Key\Value` 數據到指定的`topic`和`partition`。

- [consumer](#consumer)
- [read](#read)
- [producer](#producer)
- [write](#write)
- [transaction](#transaction)

## consumer
`consumer` 提供您查詢 consumer 的資訊
* 可以透過搜尋欄，增加查詢條件（支援Regex) 
![client_consumer](client_consumer.png)

## read
`read` 提供您資料寫入 `topic` 和 `partition` 的狀況
* 可以透過搜尋欄，增加查詢條件（支援Regex)
* records：預期取得多少幾筆資料
* timeout：可以等待多久時間（直到資料回傳）
![client_read](client_read.png)

## producer

`producer` 提供您查詢 producer 的資訊
* 可以透過搜尋欄，增加查詢條件（支援Regex)

***注意：只有 idempotent producer 的資訊可供查詢** ([Idempotent Producer介紹](https://kafka.apache.org/documentation/#producerconfigs_enable.idempotence))
![client_producer](client_producer.png)

## write
`write` 提供您單筆 `Key\Value` 寫入指定的`topic` 和 `partition`
![client_producer](client_write.png)

## transaction
`transaction` 提供您查詢 transaction 的資訊
* 可以透過搜尋欄，增加查詢條件（支援Regex)
![client_transaction](client_transaction.png)