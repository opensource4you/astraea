/beans
===

Beans 可以用來顯示伺服器端所記錄的 Java Metrics

- [查詢所有 beans](#查詢所有節點的-beans)

## 查詢所有節點的 beans
```shell
GET /beans/{domain}?{property_key=property_value}
```

cURL 範例
```shell
curl -X GET http://localhost:8001/beans/kafka.cluster?partition=22&name=UnderMinIsr
```

參數

| 名稱             | 說明                                        | 預設  |
|----------------|-------------------------------------------|-----|
| domain         | (選填) 只搜尋擁有此 domain 的 Java Metrics         | 無   |
| property_key   | (選填) 只搜尋擁有此 property_key 的 Java Metrics   | 無   |
| property_value | (選填) 只搜尋擁有此 property_value 的 Java Metrics | 無   |

JSON Response 範例
- `nodeBeans`
  - `host`: 節點名稱
  - `beans`:
    - `domainName`: domain of object name
    - `properties`: property of object name，可以與 domain 組成 object name 用來定位 Java Metrics
    - `attributes`: Java Metrics 所記錄的值，同樣是 key=value 的格式
```json
{
  "nodeBeans": [
    {
      "host": "192.168.50.168",
      "beans": [
        {
          "domainName": "kafka.cluster",
          "properties": [
            {
              "key": "partition",
              "value": "22"
            },
            {
              "key": "name",
              "value": "UnderMinIsr"
            },
            {
              "key": "topic",
              "value": "__consumer_offsets"
            },
            {
              "key": "type",
              "value": "Partition"
            }
          ],
          "attributes": [
            {
              "key": "Value",
              "value": "0"
            }
          ]
        }
      ]
    }
  ]
}
```