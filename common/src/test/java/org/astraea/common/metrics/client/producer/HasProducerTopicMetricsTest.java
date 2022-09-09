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
package org.astraea.common.metrics.client.producer;

import java.util.concurrent.ExecutionException;
import org.astraea.common.Utils;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.producer.Producer;
import org.astraea.it.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HasProducerTopicMetricsTest extends RequireSingleBrokerCluster {

  @Test
  void testClientId() throws ExecutionException, InterruptedException {
    var topic = Utils.randomString(10);
    try (var producer = Producer.of(bootstrapServers())) {
      producer.sender().topic(topic).run().toCompletableFuture().get();
      var metrics = ProducerMetrics.topics(MBeanClient.local());
      Assertions.assertNotEquals(0, metrics.stream().filter(m -> m.topic().equals(topic)).count());
      var producerTopicMetrics =
          metrics.stream().filter(m -> m.clientId().equals("producer-1")).findFirst().get();
      Assertions.assertNotEquals(0D, producerTopicMetrics.byteRate());
      Assertions.assertNotEquals(0D, producerTopicMetrics.byteTotal());
      Assertions.assertEquals(1D, producerTopicMetrics.compressionRate());
      Assertions.assertEquals(0D, producerTopicMetrics.recordErrorRate());
      Assertions.assertEquals(0D, producerTopicMetrics.recordErrorTotal());
      Assertions.assertEquals(0D, producerTopicMetrics.recordRetryRate());
      Assertions.assertEquals(0D, producerTopicMetrics.recordRetryTotal());
      Assertions.assertNotEquals(0D, producerTopicMetrics.recordSendRate());
      Assertions.assertNotEquals(0D, producerTopicMetrics.recordSendTotal());
    }
  }
}
