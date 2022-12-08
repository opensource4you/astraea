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

connection 參數

| 名稱           | 說明               | 預設  |
|--------------|------------------|-----|
| ip           | (必填) ip 地址       | 無   |
| creationRate | (必填) 每秒建立的最大連線數量 | 無上限 |

cURL 範例

將 ip 為 192.168.1.102 的 connection_creation_rate 設為 100
```shell
curl -X POST http://localhost:8001/quotas \
    -H "Content-Type: application/json" \
    -d '{
    "connection":{
      "ip": "192.168.1.102", 
      "creationRate": 100 
    }}' 
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
        "name": "connectionCreationRate",
        "value": 100
      }
    }
  ]
}
```

### 建立或變更 client consumer quotas
consumer參數

| 名稱       | 說明                           | 預設  |
|----------|------------------------------|-----|
| clientId | (必填) client id               | 無   |
| byteRate | (必填) consumer 每秒提取的最大 byte 數 | 無上限 |

cURL 範例

將 clientId 為 my-id 的 consumerByteRate 設為 100
```shell
curl -X POST http://localhost:8001/quotas \
    -H "Content-Type: application/json" \
    -d '{
    "consumer":{
      "clientId": "my-id", 
      "byteRate": 100
    }}' 
```

JSON Response 範例
```json
{
  "quotas": [
    {
      "target": {
        "name": "clientId",
        "value": "my-id"
      },
      "limit": {
        "name": "consumerByteRate",
        "value": 100
      }
    }
  ]
}
```
### 建立或變更 client producer quotas
producer參數

| 名稱       | 說明                           | 預設  |
|----------|------------------------------|-----|
| clientId | (必填) client id               | 無   |
| byteRate | (必填) producer 每秒發佈的最大 byte 數 | 無上限 |

cURL 範例

將 clientId 為 my-id 的 producerByteRate 設為 10
```shell
curl -X POST http://localhost:8001/quotas \
    -H "Content-Type: application/json" \
    -d '{
    "producer":{
      "clientId": "my-id", 
      "byteRate": 10
    }}' 
```

JSON Response 範例
```json
{
  "quotas": [
    {
      "target": {
        "name": "clientId",
        "value": "my-id"
      },
      "limit": {
        "name": "producerByteRate",
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

| 名稱       | 說明             |
|----------|----------------|
| ip       | (選填) ip 位址     |
| clientId | (選填) client id |

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
curl -X GET http://localhost:8001/quotas?clientId=my-id
```

JSON Response 範例
```json
{
  "quotas": [
    {
      "target": {
        "name": "clientId",
        "value": "my-id"
      },
      "limit": {
        "name": "consumerByteRate",
        "value": 200
      }
    },
    {
      "target": {
        "name": "clientId",
        "value": "my-id"
      },
      "limit": {
        "name": "producerByteRate",
        "value": 30
      }
    },
    {
      "target": {
        "name": "ip",
        "value": "192.168.1.102"
      },
      "limit": {
        "name": "connectionCreationRate",
        "value": 200
      }
    }
  ]
}
```