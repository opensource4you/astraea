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
import java.util.List;
import java.util.Map;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.metrics.BeanObject;
import org.astraea.app.metrics.broker.HasCount;
import org.astraea.app.metrics.broker.HasGauge;
import org.astraea.app.metrics.broker.LogMetrics;
import org.astraea.app.metrics.broker.ServerMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CostUtilsTest {
  private static final HasGauge partitionBeanObject1 =
      fakePartitionBeanObject("Log", LogMetrics.Log.SIZE.metricName(), "test-1", "0", 1000, 1000L);
  private static final HasGauge partitionBeanObject2 =
      fakePartitionBeanObject(
          "Log", LogMetrics.Log.SIZE.metricName(), "test-1", "0", 50000000, 5000L);
  private static final HasCount brokerBeanObject1 =
      fakeBrokerBeanObject(
          "BrokerTopicMetrics", ServerMetrics.Topic.BYTES_IN_PER_SEC.metricName(), 1000, 1000);
  private static final HasCount brokerBeanObject2 =
      fakeBrokerBeanObject(
          "BrokerTopicMetrics", ServerMetrics.Topic.BYTES_IN_PER_SEC.metricName(), 50000000, 5000);

  @Test
  void testDataRate() {
    var rate =
        CostUtils.dataRate(
            Map.entry(
                TopicPartition.of("test-1", 0),
                List.of(partitionBeanObject1, partitionBeanObject2)),
            "Size",
            Duration.ofSeconds(3));
    Assertions.assertEquals(11.920690536499023, rate.getValue());
  }

  @Test
  void testReplicaDataRate() {
    var rate =
        CostUtils.dataRate(
            Map.entry(0, List.of(brokerBeanObject1, brokerBeanObject2)),
            "BytesInPerSec",
            Duration.ofSeconds(3));
    Assertions.assertEquals(11.920690536499023, rate.getValue());
  }

  static LogMetrics.Log.Gauge fakePartitionBeanObject(
      String type, String name, String topic, String partition, long size, long time) {
    return new LogMetrics.Log.Gauge(
        new BeanObject(
            "kafka.log",
            Map.of("name", name, "type", type, "topic", topic, "partition", partition),
            Map.of("Value", size),
            time));
  }

  static HasCount fakeBrokerBeanObject(String type, String name, long count, long time) {
    return new ServerMetrics.Topic.Meter(
        new BeanObject(
            "kafka.server", Map.of("type", type, "name", name), Map.of("Count", count), time));
  }
}
