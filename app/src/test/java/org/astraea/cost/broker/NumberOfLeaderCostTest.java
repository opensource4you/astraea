package org.astraea.cost.broker;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.admin.Admin;
import org.astraea.admin.BeansGetter;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.FakeClusterInfo;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.BeanCollector;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.kafka.HasValue;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class NumberOfLeaderCostTest extends RequireBrokerCluster {

  @Test
  void testBrokerCost() {
    var topicName =
        List.of("ReplicaCollieTest-Path-1", "ReplicaCollieTest-Path-2", "ReplicaCollieTest-Path-3");
    try (var admin = Admin.of(bootstrapServers())) {
      var host = "localhost";
      for (var topic : topicName)
        admin.creator().topic(topic).numberOfPartitions(4).numberOfReplicas((short) 1).create();
      // wait for topic creation
      TimeUnit.SECONDS.sleep(5);
      admin.migrator().partition(topicName.get(0), 0).moveTo(List.of(0));
      admin.migrator().partition(topicName.get(0), 1).moveTo(List.of(0));
      admin.migrator().partition(topicName.get(0), 2).moveTo(List.of(0));
      admin.migrator().partition(topicName.get(0), 3).moveTo(List.of(1));
      // wait for replica migrate
      TimeUnit.SECONDS.sleep(1);
      admin.migrator().topic(topicName.get(1)).moveTo(List.of(1));
      // wait for replica migrate
      TimeUnit.SECONDS.sleep(1);
      admin.migrator().topic(topicName.get(2)).moveTo(List.of(2));

      var allBeans = new HashMap<Integer, Collection<HasBeanObject>>();
      var jmxAddress = Map.of(1001, jmxServiceURL().getPort());

      NumberOfLeaderCost costFunction = new NumberOfLeaderCost();
      jmxAddress.forEach(
          (b, port) -> {
            var firstBeanObjects =
                BeanCollector.builder()
                    .interval(Duration.ofSeconds(4))
                    .build()
                    .register()
                    .host(host)
                    .port(port)
                    .fetcher(Fetcher.of(Set.of(costFunction.fetcher())))
                    .build()
                    .current();
            allBeans.put(
                b,
                allBeans.containsKey(b)
                    ? Stream.concat(allBeans.get(b).stream(), firstBeanObjects.stream())
                        .collect(Collectors.toList())
                    : firstBeanObjects);
          });
      /*
      var clusterInfo = ClusterInfo.of(BalancerUtils.clusterSnapShot(admin), allBeans);
      costFunction
          .brokerCost(clusterInfo)
          .value()
          .forEach((broker, score) -> System.out.println(broker + ":" + score));
       */
      var loadCostFunction = new NumberOfLeaderCost();
      var load = loadCostFunction.brokerCost(exampleClusterInfo());

      Assertions.assertEquals(3.0 / (3 + 4 + 5), load.value().get(1));
      Assertions.assertEquals(4.0 / (3 + 4 + 5), load.value().get(2));
      Assertions.assertEquals(5.0 / (3 + 4 + 5), load.value().get(3));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private ClusterInfo exampleClusterInfo() {
    var LeaderCount1 =
        mockResult("ReplicaManager", KafkaMetrics.ReplicaManager.LeaderCount.metricName(), 3);
    var LeaderCount2 =
        mockResult("ReplicaManager", KafkaMetrics.ReplicaManager.LeaderCount.metricName(), 4);
    var LeaderCount3 =
        mockResult("ReplicaManager", KafkaMetrics.ReplicaManager.LeaderCount.metricName(), 5);

    Collection<HasBeanObject> broker1 = List.of(LeaderCount1);
    Collection<HasBeanObject> broker2 = List.of(LeaderCount2);
    Collection<HasBeanObject> broker3 = List.of(LeaderCount3);
    return new FakeClusterInfo() {
      @Override
      public BeansGetter beans() {
        return BeansGetter.of(Map.of(1, broker1, 2, broker2, 3, broker3));
      }
    };
  }

  private HasValue mockResult(String type, String name, long count) {
    var result = Mockito.mock(HasValue.class);
    var bean = Mockito.mock(BeanObject.class);
    Mockito.when(result.beanObject()).thenReturn(bean);
    Mockito.when(bean.getProperties()).thenReturn(Map.of("name", name, "type", type));
    Mockito.when(result.value()).thenReturn(count);
    return result;
  }
}
