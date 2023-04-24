/metricSensors
===

需要metricSensors的原因 (詳細請看[#1665](https://github.com/skiptests/astraea/pull/1665)) :

- 原來的`BalancerHandler`沒辦法長時間的收集metrics(在使用者執行PostRequest時才會收集metrics)，這會導致一些需要長時間統計metrics的`CostFunction`(例如 [add PartitionMigrateTimeCost and revise MetricSensor#fetch #1665](https://github.com/skiptests/astraea/pull/1665))無法收集足夠的metrics來計算分數
- 此將`WebService`修改成平常就可以收集metrics，以及可以隨時選擇感興趣的指標(選擇`CostFunction`)，如此變可以拉長指標的蒐集時間，並用長時間收集的metrics來計算`CostFunction`

新增metricSensors前後差異:

- 原本的`BalancerHandler`的流程如下:
  1. 使用者希望做負載平衡時打開`WebService`，此時`MetricStore`已經build，但因為沒有註冊`MetricsSensor`，因此不會撈取任何metrics
  2. 使用者送出PostRequest，此時成功註冊`MetricsSensor`並開始撈取metrics
  3. 等待撈到足夠的metrics後，`CostFunction`開始計算所需的分數
- 修改後的`MetricSensorHandler`與`BalancerHandler`的流程如下:
  1. 使用者平常就會開著`WebService`，且同時透過送出`MetricSensorHandler`的PostRequest來選擇未來可能會想要做負載平衡的`CostFunction`，此時會同時註冊這些`CostFunction`的`MetricsSensor`並開始撈取metrics
  2. 當使用者要做負載平衡時，此時叢集已經收集了一段時間的metrics，此時呼叫`BalancerHandler`的PostRequest，可以使用這些統計一段時間的metrics來做負載平衡
  3. `CostFunction`開始計算所需的分數

- [指定MetricSensors](#指定-MetricSensors)
- [查詢已指定的 MetricSensors](#查詢已指定的-MetricSensors)

## 指定 MetricSensors
```shell
GET /metricSensors
```

cURL 範例
```shell
curl -X POST http://localhost:8001/metricSensors \
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
curl -X GET http://localhost:8001/metricSensors
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
