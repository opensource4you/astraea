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
import java.util.concurrent.ExecutionException;
import org.astraea.common.admin.Admin;
import org.astraea.it.Service;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MetricPublisherTest {
  Service service = Service.builder().numberOfWorkers(0).numberOfBrokers(2).build();

  @Test
  void testParse() {
    String[] args = {
      "--bootstrap.servers",
      "localhost:9092",
      "--jmxAddress",
      "1001=localhost:8000",
      "--jmxPort",
      "7091"
    };
    var arguments = MetricPublisher.Arguments.parse(new MetricPublisher.Arguments(), args);
    Assertions.assertEquals("localhost:8000", arguments.jmxAddress.get("1001"));
  }

  @Test
  void testExecute() throws InterruptedException, ExecutionException {
    String[] args = {
      "--bootstrap.servers",
      service.bootstrapServers(),
      "--jmxPort",
      String.valueOf(service.jmxServiceURL().getPort()),
      "--ttl",
      "20s"
    };
    var arguments = MetricPublisher.Arguments.parse(new MetricPublisher.Arguments(), args);

    try (var admin = Admin.of(service.bootstrapServers())) {
      // No topic before publish
      Assertions.assertEquals(0, admin.topicNames(true).toCompletableFuture().get().size());

      // The time-to-live is 20 second, this service should terminate after 20 second
      Assertions.assertTimeout(Duration.ofSeconds(40), () -> MetricPublisher.execute(arguments));

      // Topics after publish
      Assertions.assertTrue(1 <= admin.topicNames(true).toCompletableFuture().get().size());
      var partitions =
          admin.topicNames(true).thenCompose(admin::partitions).toCompletableFuture().get();
      // There must be some metric in those topics
      Assertions.assertTrue(partitions.stream().anyMatch(p -> p.latestOffset() > 0));
    }
  }
}
