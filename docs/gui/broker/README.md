### broker

`client`頁面提供查看 `basic` `config` `metrics` 與 `alter`等資訊

- [basic](#basic)
- [config](#config)
- [metrics](#metrics)
- [alter](#alter)

### basic
`basic` 可以查詢所有或特定broker/host的基本資訊

![broker-basic 1]()
*顯示所有broker資訊*

![broker-basic 2]()
*指定特定broker id的資訊*


### config

可以查詢`broker`內的各項`config`，搜尋部分支援正規表示法，可以進一步過濾查詢資料

![broker-config 1]()
*不指定 config key*

![broker-config 2]()
*指定兩個key的寫法*


### metrics

可以取得`broker`的各項指標，搜尋可支援正規表示法

指標種類有以下項目:

- host
- controller
- controller state
- network
- delayed operation
- replica
- broker topic

![broker-metrics 1]()
*查看 host 的資訊*

### alter

可以在此頁面檢視與修改 `broker` 的設定

![broker-alter 1]()
*快速查看 broker 資訊，並且可以透過下方按鈕變更*