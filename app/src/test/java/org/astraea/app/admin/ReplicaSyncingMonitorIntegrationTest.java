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
package org.astraea.app.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.astraea.app.argument.Argument;
import org.astraea.app.common.Utils;
import org.astraea.app.producer.Producer;
import org.astraea.app.producer.Sender;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Test;

class ReplicaSyncingMonitorIntegrationTest extends RequireBrokerCluster {

  private static final String TOPIC_NAME =
      ReplicaSyncingMonitorIntegrationTest.class.getSimpleName();
  private static final byte[] dummyBytes = new byte[1024];

  @Test
  void execute() throws InterruptedException {
    // arrange
    try (Admin topicAdmin = Admin.of(bootstrapServers())) {
      topicAdmin
          .creator()
          .topic(TOPIC_NAME)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 1)
          .create();

      Sender<byte[], byte[]> sender =
          Producer.of(bootstrapServers()).sender().topic(TOPIC_NAME).partition(0).value(dummyBytes);

      // create 16MB of data
      IntStream.range(0, 16 * 1024)
          .mapToObj(i -> sender.run().toCompletableFuture())
          .forEach(CompletableFuture::join);

      int currentBroker =
          topicAdmin
              .replicas(Set.of(TOPIC_NAME))
              .get(TopicPartition.of(TOPIC_NAME, 0))
              .get(0)
              .broker();
      int moveToBroker = (currentBroker + 1) % logFolders().size();

      Thread executionThread =
          new Thread(
              () -> {
                topicAdmin.migrator().partition(TOPIC_NAME, 0).moveTo(List.of(moveToBroker));
                ReplicaSyncingMonitor.execute(
                    topicAdmin,
                    Argument.parse(
                        new ReplicaSyncingMonitor.Argument(),
                        new String[] {
                          "--bootstrap.servers",
                          bootstrapServers(),
                          "--topic",
                          TOPIC_NAME,
                          "--interval",
                          "0.1"
                        }));
              });

      // act
      executionThread.start();
      TimeUnit.SECONDS.timedJoin(executionThread, 8); // wait until the thread exit
      Utils.sleep(Duration.ofSeconds(2)); // sleep 2 extra seconds to ensure test run in stable

      // assert
      assertSame(Thread.State.TERMINATED, executionThread.getState());
      assertEquals(
          1, topicAdmin.replicas(Set.of(TOPIC_NAME)).get(TopicPartition.of(TOPIC_NAME, 0)).size());
      assertEquals(
          moveToBroker,
          topicAdmin.replicas(Set.of(TOPIC_NAME)).get(TopicPartition.of(TOPIC_NAME, 0)).stream()
              .filter(Replica::leader)
              .findFirst()
              .orElseThrow()
              .broker());
    }
  }
}
