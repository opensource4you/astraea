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
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.MetricsTestUtil;
import org.astraea.it.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

public class LogMetricsTest extends RequireSingleBrokerCluster {

  @ParameterizedTest
  @EnumSource(LogMetrics.LogCleanerManager.class)
  void testLogCleanerManager(LogMetrics.LogCleanerManager log) {
    var topicName = Utils.randomString(10);
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      var beans =
          log.fetch(MBeanClient.local()).stream()
              .collect(Collectors.groupingBy(LogMetrics.LogCleanerManager.Gauge::path));
      Assertions.assertEquals(
          dataFolders().values().stream().flatMap(Collection::stream).distinct().count(),
          beans.size());
      dataFolders().values().stream()
          .flatMap(Collection::stream)
          .distinct()
          .forEach(
              d ->
                  Assertions.assertTrue(
                      beans.containsKey(d), "beans: " + beans.keySet() + " d:" + d));
      //      dataFolders().values().forEach(ds ->
      // Assertions.assertTrue(beans.keySet().containsAll(ds), "beans: " + beans.keySet() + " ds: "
      // + ds));
    }
  }

  @ParameterizedTest
  @EnumSource(LogMetrics.Log.class)
  void testMetrics(LogMetrics.Log log) throws ExecutionException, InterruptedException {
    var topicName = Utils.randomString(10);
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(2).run().toCompletableFuture().get();
      Utils.sleep(Duration.ofSeconds(2));
      var beans =
          log.fetch(MBeanClient.local()).stream()
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
    log.fetch(MBeanClient.local())
        .forEach(
            m -> {
              MetricsTestUtil.validate(m);
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
  void testTopicPartitionMetrics(LogMetrics.Log request)
      throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      // there are only 3 brokers, so 10 partitions can make each broker has some partitions
      admin
          .creator()
          .topic(Utils.randomString(5))
          .numberOfPartitions(10)
          .run()
          .toCompletableFuture()
          .get();
    }

    // wait for topic creation
    Utils.sleep(Duration.ofSeconds(2));

    var beans = request.fetch(MBeanClient.local());
    assertNotEquals(0, beans.size());
  }

  @Test
  void testAllEnumNameUnique() {
    Assertions.assertTrue(
        MetricsTestUtil.metricDistinct(LogMetrics.Log.values(), LogMetrics.Log::metricName));
  }
}
