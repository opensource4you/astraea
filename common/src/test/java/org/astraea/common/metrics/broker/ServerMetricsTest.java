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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.astraea.common.Utils;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.MetricsTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class ServerMetricsTest {

  @ParameterizedTest()
  @EnumSource(value = ServerMetrics.DelayedOperationPurgatory.class)
  void testPurgatorySize(ServerMetrics.DelayedOperationPurgatory request) {
    request.fetch(MBeanClient.local()).forEach(MetricsTestUtil::validate);
  }

  @Test
  void testKafkaMetricsOf() {
    Arrays.stream(ServerMetrics.Topic.values())
        .forEach(
            t ->
                Assertions.assertEquals(
                    t, ServerMetrics.Topic.of(t.metricName().toLowerCase(Locale.ROOT))));
    Arrays.stream(ServerMetrics.Topic.values())
        .forEach(
            t ->
                Assertions.assertEquals(
                    t, ServerMetrics.Topic.of(t.metricName().toUpperCase(Locale.ROOT))));
    assertThrows(IllegalArgumentException.class, () -> ServerMetrics.Topic.of("nothing"));
  }

  @ParameterizedTest
  @EnumSource(ServerMetrics.Topic.class)
  void testBrokerTopic(ServerMetrics.Topic brokerTopic) {
    var object =
        new ServerMetrics.Topic.Meter(
            new BeanObject("object", Map.of("name", brokerTopic.metricName()), Map.of()));
    Assertions.assertEquals(1, brokerTopic.of(List.of(object)).size());

    Assertions.assertEquals(
        0,
        brokerTopic
            .of(
                List.of(
                    new ServerMetrics.Topic.Meter(
                        new BeanObject(
                            "object", Map.of("name", Utils.randomString(10)), Map.of()))))
            .size());
  }

  @Test
  void testAllEnumNameUnique() {
    Assertions.assertTrue(
        MetricsTestUtil.metricDistinct(
            ServerMetrics.ReplicaManager.values(), ServerMetrics.ReplicaManager::metricName));
    Assertions.assertTrue(
        MetricsTestUtil.metricDistinct(
            ServerMetrics.DelayedOperationPurgatory.values(),
            ServerMetrics.DelayedOperationPurgatory::metricName));
    Assertions.assertTrue(
        MetricsTestUtil.metricDistinct(
            ServerMetrics.Topic.values(), ServerMetrics.Topic::metricName));
  }
}
