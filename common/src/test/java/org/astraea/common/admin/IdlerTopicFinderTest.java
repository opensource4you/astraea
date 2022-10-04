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
package org.astraea.common.admin;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.producer.Producer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IdlerTopicFinderTest extends RequireBrokerCluster {
  @Test
  void testLatestTimestamp() throws InterruptedException {
    try (var producer = Producer.builder().bootstrapServers(bootstrapServers()).build()) {
      producer.sender().topic("produce").value("1".getBytes()).run().toCompletableFuture().get();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }

    try (var admin = Admin.of(bootstrapServers())) {
      var finder = admin.idleTopicFinder();
      finder.clearChecker();
      finder.addChecker(IdleTopicFinder.Checker.latestTimestamp(Duration.ofSeconds(3)));
      Assertions.assertEquals(Set.of(), finder.idleTopics());
      Thread.sleep(3000);
      Assertions.assertEquals(Set.of("produce"), finder.idleTopics());
    }
  }

  @Test
  void testNoAssignment() throws InterruptedException {
    var consumer =
        Consumer.forTopics(Set.of("produce"))
            .fromBeginning()
            .bootstrapServers(bootstrapServers())
            .build();
    var consumerThread = new Thread(() -> consumer.poll(Duration.ofSeconds(5)));
    consumerThread.start();
    try (var admin = Admin.of(bootstrapServers())) {
      var finder = admin.idleTopicFinder();
      finder.clearChecker();
      finder.addChecker(IdleTopicFinder.Checker.noAssignment());
      Thread.sleep(5000);

      Assertions.assertEquals(Set.of(), finder.idleTopics());
      consumerThread.join();
      consumer.close();
      Assertions.assertEquals(Set.of("produce"), finder.idleTopics());
    }
  }
}
