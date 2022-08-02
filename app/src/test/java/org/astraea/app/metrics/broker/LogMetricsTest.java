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
package org.astraea.app.metrics.broker;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.MBeanClient;
import org.astraea.app.service.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

public class LogMetricsTest extends RequireSingleBrokerCluster {

  @ParameterizedTest
  @EnumSource(LogMetrics.Log.class)
  void testMetrics(LogMetrics.Log log) {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(2).create();
      Utils.sleep(Duration.ofSeconds(2));
      var beans =
          log.fetch(MBeanClient.local()).stream()
              .filter(m -> m.topic().equals(topicName))
              .collect(Collectors.toUnmodifiableList());
      Assertions.assertEquals(2, beans.size());
      Assertions.assertEquals(
          2,
          beans.stream().map(LogMetrics.Log.Meter::partition).collect(Collectors.toSet()).size());
      beans.forEach(m -> Assertions.assertEquals(m.type(), log));
    }
  }

  @ParameterizedTest
  @EnumSource(LogMetrics.Log.class)
  void testValue(LogMetrics.Log log) {
    log.fetch(MBeanClient.local()).forEach(m -> Assertions.assertTrue(m.value() >= 0));
    log.fetch(MBeanClient.local()).forEach(m -> Assertions.assertEquals(m.type(), log));
  }

  @Test
  void testMeters() {
    var other = Mockito.mock(HasBeanObject.class);
    var target = Mockito.mock(LogMetrics.Log.Meter.class);
    Mockito.when(target.type()).thenReturn(LogMetrics.Log.LOG_END_OFFSET);
    var result = LogMetrics.Log.meters(List.of(other, target), LogMetrics.Log.LOG_END_OFFSET);
    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(target, result.iterator().next());
    Assertions.assertEquals(
        0, LogMetrics.Log.meters(List.of(other, target), LogMetrics.Log.SIZE).size());
  }

  @ParameterizedTest()
  @EnumSource(value = LogMetrics.Log.class)
  void testTopicPartitionMetrics(LogMetrics.Log request) {
    try (var admin = Admin.of(bootstrapServers())) {
      // there are only 3 brokers, so 10 partitions can make each broker has some partitions
      admin.creator().topic(Utils.randomString(5)).numberOfPartitions(10).create();
    }

    // wait for topic creation
    Utils.sleep(Duration.ofSeconds(2));

    var beans = request.fetch(MBeanClient.local());
    assertNotEquals(0, beans.size());
  }
}
