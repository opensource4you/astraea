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
package org.astraea.common.metrics.broker;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.JndiClient;
import org.astraea.common.metrics.MetricsTestUtils;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

public class LogMetricsTest {
  private static final Service SERVICE = Service.builder().numberOfBrokers(1).build();

  @BeforeAll
  static void createBroker() {
    // call broker-related method to initialize broker cluster
    Assertions.assertNotEquals(0, SERVICE.dataFolders().size());
  }

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @ParameterizedTest
  @EnumSource(LogMetrics.LogCleanerManager.class)
  void testLogCleanerManager(LogMetrics.LogCleanerManager log) {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      var beans =
          log.fetch(JndiClient.local()).stream()
              .collect(Collectors.groupingBy(LogMetrics.LogCleanerManager.Gauge::path));
      Assertions.assertEquals(
          SERVICE.dataFolders().values().stream().flatMap(Collection::stream).distinct().count(),
          beans.size());
      SERVICE.dataFolders().values().stream()
          .flatMap(Collection::stream)
          .distinct()
          .forEach(
              d ->
                  Assertions.assertTrue(
                      beans.containsKey(d), "beans: " + beans.keySet() + " d:" + d));
    }
  }

  @ParameterizedTest
  @EnumSource(LogMetrics.Log.class)
  void testMetrics(LogMetrics.Log log) {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(2).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));
      var beans =
          log.fetch(JndiClient.local()).stream()
              .filter(m -> m.topic().equals(topicName))
              .collect(Collectors.toUnmodifiableList());
      Assertions.assertEquals(2, beans.size());
      Assertions.assertEquals(
          2,
          beans.stream().map(LogMetrics.Log.Gauge::partition).collect(Collectors.toSet()).size());
      beans.forEach(m -> Assertions.assertEquals(m.type(), log));
    }
  }

  @ParameterizedTest
  @EnumSource(LogMetrics.Log.class)
  void testValue(LogMetrics.Log log) {
    log.fetch(JndiClient.local())
        .forEach(
            m -> {
              MetricsTestUtils.validate(m);
              Assertions.assertEquals(m.type(), log);
            });
  }

  @Test
  void testGauges() {
    var other = Mockito.mock(HasBeanObject.class);
    var target = Mockito.mock(LogMetrics.Log.Gauge.class);
    Mockito.when(target.type()).thenReturn(LogMetrics.Log.LOG_END_OFFSET);
    var result = LogMetrics.Log.gauges(List.of(other, target), LogMetrics.Log.LOG_END_OFFSET);
    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(target, result.iterator().next());
    Assertions.assertEquals(
        0, LogMetrics.Log.gauges(List.of(other, target), LogMetrics.Log.SIZE).size());
  }

  @ParameterizedTest()
  @EnumSource(value = LogMetrics.Log.class)
  void testTopicPartitionMetrics(LogMetrics.Log request) {
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      // there are only 3 brokers, so 10 partitions can make each broker has some partitions
      admin
          .creator()
          .topic(Utils.randomString(5))
          .numberOfPartitions(10)
          .run()
          .toCompletableFuture()
          .join();
    }

    // wait for topic creation
    Utils.sleep(Duration.ofSeconds(2));

    var beans = request.fetch(JndiClient.local());
    assertNotEquals(0, beans.size());
  }

  @Test
  void testAllEnumNameUnique() {
    Assertions.assertTrue(
        MetricsTestUtils.metricDistinct(LogMetrics.Log.values(), LogMetrics.Log::metricName));
  }
}
