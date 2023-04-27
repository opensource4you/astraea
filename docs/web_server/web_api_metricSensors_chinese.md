/metricSensors
===

此api用來指定未來可能會想要使用的`Costfunction`之`MetricSensors`，主要功能如下 (相關討論請看[#1665](https://github.com/skiptests/astraea/pull/1665)) :

- [指定MetricSensors](#指定-MetricSensors): 選擇要使用的`Costfunction`之`MetricSensor`
- [查詢已指定的 MetricSensors](#查詢已指定的-MetricSensors): 查看當前已指定之`MetricSensor`的`CostFunction`

## 指定 MetricSensors
```shell
GET /sensors
```

cURL 範例
```shell
curl -X POST http://localhost:8001/sensors \
    -H "Content-Type: application/json" \
    -d '{
        "costs": [
                "org.astraea.common.cost.ReplicaLeaderCost",  
                "org.astraea.common.cost.NetworkIngressCost",
                "org.astraea.common.cost.NetworkEgressCost"
        ]
     }'
```

JSON Response 範例
- `costs`: 目前已經註冊的`MetricSensors`之`Costfunction`，`MetricStore`會根據這些`MetricSensors`去撈取所需的metrics
```json
{
   "costs":[
      "org.astraea.common.cost.NetworkIngressCost",
      "org.astraea.common.cost.ReplicaLeaderCost",
      "org.astraea.common.cost.NetworkEgressCost"
   ]
}
```

## 查詢已指定的 MetricSensors

```shell
GET /metricSensors
```

cURL 範例

查詢已經註冊的`MetricSensors`之`Costfunction`
```shell
curl -X GET http://localhost:8001/sensors
```

JSON Response 範例
 ```json
{
   "costs":[
      "org.astraea.common.cost.NetworkCost$$Lambda$478/0x0000000840297840",
      "org.astraea.common.cost.ReplicaLeaderCost$$Lambda$476/0x0000000840297040"
   ]
}
 ```
