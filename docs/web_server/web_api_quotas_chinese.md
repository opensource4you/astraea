/quotas
===

- [建立/變更 quotas](#建立或變更-quotas)
  - [建立/變更 ip quotas](#建立或變更-ip-quotas)
  - [建立/變更 client quotas](#建立或變更-client-quotas)
- [查詢 quotas](#查詢-quotas)

## 建立或變更 quotas
```shell
POST /quotas
```
以 ip 或是 client 為目標對象來增設 quotas

### 建立或變更 ip quotas

參數

| 名稱                       | 說明               | 預設  |
|--------------------------|------------------|-----|
| ip                       | (必填) ip 地址       | 無   |
| connection_creation_rate | (選填) 每秒建立的最大連線數量 | 無上限 |
- 除了 ip 以外的參數都不填寫的話，quotas 不會建立

cURL 範例

將 ip 為 192.168.1.102 的 connection_creation_rate 設為 100
```shell
curl -X POST http://localhost:8001/quotas \
    -H "Content-Type: application/json" \
    -d '{
    "ip": "192.168.1.102", 
    "connection_creation_rate": 100 
    }' 
```

JSON Response 範例
```json
{
  "quotas": [
    {
      "target": {
        "name": "ip",
        "value": "192.168.1.102"
      },
      "limit": {
        "name": "connection_creation_rate",
        "value": 100
      }
    }
  ]
}
```

### 建立或變更 client quotas
參數

| 名稱                 | 說明                           | 預設  |
|--------------------|------------------------------|-----|
| client-id          | (必填) client id               | 無   |
| producer_byte_rate | (選填) producer 每秒發佈的最大 byte 數 | 無上限 |
| consumer_byte_rate | (選填) consumer 每秒提取的最大 byte 數 | 無上限 |
- 除了 client-id 以外的參數都不填寫的話，quotas 不會建立

cURL 範例

將 client-id 為 my-id 的 producer_byte_rate 設為 10，且 consumer_byte_rate 設為 100
```shell
curl -X POST http://localhost:8001/quotas \
    -H "Content-Type: application/json" \
    -d '{
    "client-id": "my-id", 
    "consumer_byte_rate": 100
    "producer_byte_rate": 10
    }' 
```

JSON Response 範例
```json
{
  "quotas": [
    {
      "target": {
        "name": "client-id",
        "value": "my-id"
      },
      "limit": {
        "name": "consumer_byte_rate",
        "value": 100
      }
    },
    {
      "target": {
        "name": "client-id",
        "value": "my-id"
      },
      "limit": {
        "name": "producer_byte_rate",
        "value": 10
      }
    }
  ]
}
```

## 查詢 quotas
```shell
GET /quotas
```
參數

| 名稱        | 說明             |
|-----------|----------------|
| ip        | (選填) ip 位址     |
| client-id | (選填) client id |

cURL 範例

查詢所有 quotas
```shell
curl -X GET http://localhost:8001/quotas
```

查詢 ip 為 192.168.1.102 的 quotas
```shell
curl -X GET http://localhost:8001/quotas?ip=192.168.1.102
```

查詢 client-id 為 my-id 的 quotas
```shell
curl -X GET http://localhost:8001/quotas?client-id=my-id
```

JSON Response 範例
```json
{
  "quotas": [
    {
      "target": {
        "name": "client-id",
        "value": "my-id"
      },
      "limit": {
        "name": "consumer_byte_rate",
        "value": 200
      }
    },
    {
      "target": {
        "name": "client-id",
        "value": "my-id"
      },
      "limit": {
        "name": "producer_byte_rate",
        "value": 30
      }
    },
    {
      "target": {
        "name": "ip",
        "value": "192.168.1.102"
      },
      "limit": {
        "name": "connection_creation_rate",
        "value": 200
      }
    }
  ]
}
```