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
import java.util.NoSuchElementException;
import java.util.function.BiConsumer;
import org.astraea.common.Utils;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.common.producer.Serializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class InternalTopicCollectorTest extends AbstractMetricCollectorTest {

  @Override
  protected MetricCollector collector(Map<MetricSensor, BiConsumer<Integer, Exception>> sensors) {
    return MetricCollector.internalTopic()
        .bootstrapServer(SERVICE.bootstrapServers())
        .addMetricSensors(sensors)
        .build();
  }

  @BeforeAll
  static void addMetricToTopic() {
    // Manually produce one bean to the internal topic
    var memBean = new BeanObject("java.lang", Map.of("type", "Memory"), Map.of());
    var osBean = new BeanObject("java.lang", Map.of("type", "OperatingSystem"), Map.of());
    var byteInBean =
        new BeanObject(
            "kafka.server",
            Map.of("type", "BrokerTopicMetrics", "name", "BytesInPerSec"),
            Map.of());
    try (var producer =
        Producer.builder()
            .bootstrapServers(SERVICE.bootstrapServers())
            .valueSerializer(Serializer.STRING)
            .build()) {
      // send all three metrics to all corresponding topic
      SERVICE.dataFolders().keySet().stream()
          .map(
              id ->
                  producer
                      .send(Record.builder().topic(idToTopic(id)).value(memBean.toString()).build())
                      .toCompletableFuture())
          .forEach(future -> Utils.swallowException(future::get));
      SERVICE.dataFolders().keySet().stream()
          .map(
              id ->
                  producer
                      .send(Record.builder().topic(idToTopic(id)).value(osBean.toString()).build())
                      .toCompletableFuture())
          .forEach(future -> Utils.swallowException(future::get));
      SERVICE.dataFolders().keySet().stream()
          .map(
              id ->
                  producer
                      .send(
                          Record.builder()
                              .topic(idToTopic(id))
                              .value(byteInBean.toString())
                              .build())
                      .toCompletableFuture())
          .forEach(future -> Utils.swallowException(future::get));
    }
  }

  private static String idToTopic(int id) {
    return "__" + id + "_broker_metrics";
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
      Assertions.assertThrows(NoSuchElementException.class, () -> metricStore.beans(query));
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
      Assertions.assertEquals(targetBean, metricStore.beans(query).stream().findAny().orElse(null));
    }
  }
}
