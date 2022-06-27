/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.cost;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.collector.BeanCollector;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.jmx.BeanObject;
import org.astraea.app.metrics.kafka.HasValue;
import org.astraea.app.metrics.kafka.KafkaMetrics;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class NumberOfLeaderCostTest extends RequireBrokerCluster {

  @Test
  void testGetMetrics() {
    restartCluster(1);
    var topicName = List.of("testGetMetrics-1", "testGetMetrics-2", "testGetMetrics-3");
    try (var admin = Admin.of(bootstrapServers())) {
      var host = "localhost";
      admin
          .creator()
          .topic(topicName.get(0))
          .numberOfPartitions(4)
          .numberOfReplicas((short) 1)
          .create();
      admin
          .creator()
          .topic(topicName.get(1))
          .numberOfPartitions(5)
          .numberOfReplicas((short) 1)
          .create();
      admin
          .creator()
          .topic(topicName.get(2))
          .numberOfPartitions(6)
          .numberOfReplicas((short) 1)
          .create();
      // wait for topic creation
      TimeUnit.SECONDS.sleep(5);

      NumberOfLeaderCost costFunction = new NumberOfLeaderCost();
      var beanObjects =
          BeanCollector.builder()
              .interval(Duration.ofSeconds(4))
              .build()
              .register()
              .host(host)
              .port(jmxServiceURL().getPort())
              .fetcher(Fetcher.of(Set.of(costFunction.fetcher())))
              .build()
              .current();
      var leaderNum =
          beanObjects.stream()
              .filter(x -> x instanceof HasValue)
              .filter(x -> x.beanObject().getProperties().get("name").equals("LeaderCount"))
              .filter(x -> x.beanObject().getProperties().get("type").equals("ReplicaManager"))
              .sorted(Comparator.comparing(HasBeanObject::createdTimestamp).reversed())
              .map(x -> (HasValue) x)
              .limit(1)
              .map(e2 -> (int) e2.value())
              .collect(Collectors.toList());
      Assertions.assertEquals(leaderNum.get(0), 4 + 5 + 6);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

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
      public ClusterBean clusterBean() {
        return ClusterBean.of(Map.of(1, broker1, 2, broker2, 3, broker3));
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
