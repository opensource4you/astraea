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
package org.astraea.app.metrics.producer;

import java.util.concurrent.ExecutionException;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.KafkaMetrics;
import org.astraea.app.metrics.jmx.MBeanClient;
import org.astraea.app.producer.Producer;
import org.astraea.app.service.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HasProducerNodeMetricsTest extends RequireSingleBrokerCluster {

  @Test
  void testAttributes() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    try (var producer = Producer.of(bootstrapServers())) {
      producer.sender().topic(topic).run().toCompletableFuture().get();
      var metrics = KafkaMetrics.Producer.node(MBeanClient.local(), brokerIds().iterator().next());
      Assertions.assertEquals(1, metrics.size());
      var producerNodeMetrics = metrics.get("producer-1");
      Assertions.assertNotEquals(0D, producerNodeMetrics.incomingByteRate());
      Assertions.assertNotEquals(0D, producerNodeMetrics.incomingByteTotal());
      Assertions.assertNotEquals(0D, producerNodeMetrics.outgoingByteRate());
      Assertions.assertNotEquals(0D, producerNodeMetrics.outgoingByteTotal());
      Assertions.assertNotEquals(0D, producerNodeMetrics.requestLatencyAvg());
      Assertions.assertNotEquals(0D, producerNodeMetrics.requestLatencyMax());
      Assertions.assertNotEquals(0D, producerNodeMetrics.requestRate());
      Assertions.assertNotEquals(0D, producerNodeMetrics.requestSizeAvg());
      Assertions.assertNotEquals(0D, producerNodeMetrics.requestSizeMax());
      Assertions.assertNotEquals(0D, producerNodeMetrics.requestTotal());
      Assertions.assertNotEquals(0D, producerNodeMetrics.responseRate());
      Assertions.assertNotEquals(0D, producerNodeMetrics.responseTotal());
    }
  }
}
