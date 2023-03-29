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

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.astraea.common.admin.Admin;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TopicMetricTest {
  static final Service SERVICE = Service.builder().numberOfWorkers(0).build();

  @BeforeAll
  static void createTopic() {
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin.creator().topic("__1_broker_metrics").run().toCompletableFuture().get();
    } catch (ExecutionException | InterruptedException e) {
      Assertions.fail("Topic creation error");
    }
  }

  @AfterAll
  static void close() {
    SERVICE.close();
  }

  @Test
  void test() {
    try (var receiver = MetricsStore.Receiver.topic(SERVICE.bootstrapServers())) {
      Assertions.assertEquals(Map.of(), receiver.receive(Duration.ofSeconds(1)));

      try (var admin = Admin.of(SERVICE.bootstrapServers())) {
        var ids = admin.consumerGroupIds().toCompletableFuture().get();
        Assertions.assertEquals(1, ids.size());
      } catch (InterruptedException | ExecutionException e) {
        Assertions.fail("Admin group id fetching error");
      }
    }
  }
}
