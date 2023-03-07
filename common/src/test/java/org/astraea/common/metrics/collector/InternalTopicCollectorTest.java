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
package org.astraea.common.metrics.collector;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.common.producer.Serializer;
import org.astraea.it.Service;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InternalTopicCollectorTest {
  Service SERVICE = Service.builder().numberOfWorkers(0).build();

  @Test
  void testCollect() {
    // Manually produce one bean to the internal topic
    var bean =
        new BeanObject(
            "kafka.log",
            Map.of("type", "Log", "topic", "t1", "partition", "0", "name", "Size"),
            Map.of());
    try (var producer =
        Producer.builder()
            .bootstrapServers(SERVICE.bootstrapServers())
            .valueSerializer(Serializer.STRING)
            .build()) {
      producer
          .send(Record.builder().topic("__1001_broker_metrics").value(bean.toString()).build())
          .toCompletableFuture()
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    try (var collector =
        MetricCollector.internalTopic()
            .bootstrapServer(SERVICE.bootstrapServers())
            .addMetricSensor((client, ignore) -> LogMetrics.Log.SIZE.fetch(client))
            .build()) {
      Thread.sleep(5000);
      // Check if collector get the beans
      Assertions.assertEquals(1, collector.clusterBean().all().size());
      Assertions.assertEquals(
          bean.toString(),
          collector.clusterBean().all().values().stream().findAny().get().stream()
              .findAny()
              .get()
              .beanObject()
              .toString());
      Assertions.assertEquals(
          bean.toString(), collector.metrics().findAny().get().beanObject().toString());
      Assertions.assertEquals(Set.of(1001), collector.listIdentities());
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Test
  void testIllegalArgument() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> MetricCollector.internalTopic().build());
  }

  @Test
  void testMetricStore() {
    try (var metricStore = new InternalTopicCollector.MetricStore()) {
      var domainName = "test";
      var properties = Map.of("name", "n", "type", "t");
      var query = BeanQuery.builder().domainName(domainName).properties(properties).build();

      // No beans in metric store initially
      Assertions.assertTrue(metricStore.beans(query).isEmpty());
      // Put some beans
      var targetBean = new BeanObject(domainName, properties, Map.of());
      var otherBean = new BeanObject("others", Map.of(), Map.of());
      metricStore.put(
          new InternalTopicCollector.BeanProperties(
              targetBean.domainName(), targetBean.properties()),
          targetBean);
      metricStore.put(
          new InternalTopicCollector.BeanProperties(otherBean.domainName(), otherBean.properties()),
          otherBean);
      Assertions.assertEquals(1, metricStore.beans(query).size());
      Assertions.assertEquals(targetBean, metricStore.beans(query).stream().findAny().get());
    }
  }
}
