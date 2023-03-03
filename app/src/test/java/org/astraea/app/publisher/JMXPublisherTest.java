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
package org.astraea.app.publisher;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.astraea.common.admin.Admin;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.consumer.SeekStrategy;
import org.astraea.common.metrics.BeanObject;
import org.astraea.it.Service;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JMXPublisherTest {
  Service service = Service.builder().numberOfWorkers(0).build();

  @Test
  void testPublish() throws InterruptedException, ExecutionException {
    var testBean = new BeanObject("java.lang", Map.of("name", "n1"), Map.of("value", "v1"));
    try (var jmxPublisher = JMXPublisher.create(service.bootstrapServers())) {
      jmxPublisher.publish("1", testBean);

      // Test topic creation
      try (var admin = Admin.of(service.bootstrapServers())) {
        var topics = admin.topicNames(false).toCompletableFuture().get();
        Assertions.assertEquals(1, topics.size());
        Assertions.assertEquals("__1_broker_metrics", topics.stream().findAny().get());
      }

      // Test record published
      try (var consumer =
          Consumer.forTopics(Set.of("__1_broker_metrics"))
              .bootstrapServers(service.bootstrapServers())
              .valueDeserializer(Deserializer.STRING)
              .seek(SeekStrategy.DISTANCE_FROM_BEGINNING, 0)
              .build()) {
        var records = consumer.poll(Duration.ofSeconds(5));
        Assertions.assertEquals(1, records.size());
        Assertions.assertEquals(testBean.toString(), records.get(0).value());
      }
    }
  }
}
