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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.astraea.app.common.Utils;
import org.astraea.app.consumer.Consumer;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TopicExplorerTest extends RequireBrokerCluster {

  // List of created consumer work.
  // Each consumer has to keep fetching from the broker to prevent been killed.
  // The `TopicExploreTest#consumer` created a thread that keep doing the fetching job.
  // This list keeps the thread works, shutdown these thread when necessary.
  private final List<Closeable> createdConsumerWorks = new ArrayList<>();

  @AfterEach
  private void closeConsumers() {
    createdConsumerWorks.forEach(
        x -> {
          try {
            x.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        });
    createdConsumerWorks.clear();
  }

  private void consumer(Set<String> topics, String groupName, String instanceName) {
    // start the consuming work and insert the new consumer work into the list
    createdConsumerWorks.add(
        new Closeable() {
          private final AtomicBoolean shutdown = new AtomicBoolean(false);

          public Closeable start() {
            new Thread(
                    () -> {
                      var consumer =
                          Consumer.forTopics(topics)
                              .bootstrapServers(bootstrapServers())
                              .groupId(groupName)
                              .configs(
                                  Map.of(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceName))
                              .fromBeginning()
                              .build();

                      while (!shutdown.get()) consumer.poll(Duration.ofSeconds(1));

                      consumer.close();
                    })
                .start();
            return this;
          }

          @Override
          public void close() {
            shutdown.set(true);
          }
        }.start());
  }

  @Test
  void testExecute() {
    // arrange
    var topicName = "TopicExplorerTest-testExecute-Topic";
    var groupName = "TopicExplorerTest-testExecute-ConsumerGroup";
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(3).numberOfReplicas((short) 1).create();
      consumer(Set.of(topicName), groupName, "alpha");
      consumer(Set.of(topicName), groupName, "beta");
      consumer(Set.of(topicName), groupName, "gamma");

      Utils.sleep(Duration.ofSeconds(5)); // wait for broker process resource creation

      // act
      var result = TopicExplorer.execute(admin, Set.of(topicName));

      // assert
      assertTrue(
          ChronoUnit.SECONDS.between(result.time, LocalDateTime.now()) < 10,
          "assert the execution timestamp come from a reasonable passed time (10sec before)");
      assertTrue(result.consumerGroups.containsKey(groupName));
      assertEquals(3, result.consumerGroups.get(groupName).activeMembers().size());
      assertTrue(
          result.consumerGroups.get(groupName).activeMembers().stream()
              .anyMatch(x -> x.groupInstanceId().orElseThrow().equals("alpha")));
      assertTrue(
          result.consumerGroups.get(groupName).activeMembers().stream()
              .anyMatch(x -> x.groupInstanceId().orElseThrow().equals("beta")));
      assertTrue(
          result.consumerGroups.get(groupName).activeMembers().stream()
              .anyMatch(x -> x.groupInstanceId().orElseThrow().equals("gamma")));
      assertTrue(result.partitionInfo.containsKey(topicName));
      assertEquals(3, result.partitionInfo.get(topicName).size());
    }
  }

  @Test
  void testOutput() {
    // arrange
    var now = LocalDateTime.now();
    var mockOutput = new ByteArrayOutputStream();
    var printStream = new PrintStream(mockOutput);
    var groupMembers =
        List.of(
            new Member(
                "my-consumer-group-1",
                "memberId-1",
                Optional.of("instance-1"),
                "clientId-1",
                "host1"),
            new Member(
                "my-consumer-group-1",
                "memberId-2",
                Optional.of("instance-2"),
                "clientId-2",
                "host2"),
            new Member(
                "my-consumer-group-1",
                "memberId-3",
                Optional.of("instance-3"),
                "clientId-3",
                "host3"));
    var partitionInfo =
        List.of(
            new TopicExplorer.PartitionInfo(
                new TopicPartition("my-topic", 0),
                0,
                100,
                List.of(new Replica(55, 15, 100, true, false, true, false, true, "/tmp/path0"))));
    var result =
        new TopicExplorer.Result(
            now,
            Map.of("my-topic", partitionInfo),
            Map.of(
                "my-consumer-group-1",
                new ConsumerGroup(
                    "my-consumer-group-1",
                    groupMembers,
                    Map.of(new TopicPartition("my-topic", 0), 50L),
                    Map.of(groupMembers.get(0), Set.of(new TopicPartition("my-topic", 0))))));

    // act
    TopicExplorer.TreeOutput.print(result, printStream);

    // assert
    final String output = mockOutput.toString();
    System.out.println(output);
    assertTrue(output.matches("(?ms).+Topic.+my-topic.+"), "topic name printed");
    assertTrue(
        output.matches("(?ms).+Consumer Group.+my-consumer-group.+"),
        "consumer group name printed");
    assertTrue(output.matches("(?ms).+offset.+0/50/100.+"), "offset printed");
    assertTrue(output.matches("(?ms).+member.+memberId-1.+"), "member info printed");
    assertTrue(output.matches("(?ms).+member.+memberId-2.+"), "member info printed");
    assertTrue(output.matches("(?ms).+member.+memberId-3.+"), "member info printed");
    assertTrue(output.matches("(?ms).+working on partition 0.+"), "assignment printed");
    assertTrue(output.matches("(?ms).+no partition assigned.+"), "assignment printed");
    assertTrue(output.matches("(?ms).+clientId.+clientId-1.+"), "host printed");
    assertTrue(output.matches("(?ms).+clientId.+clientId-2.+"), "host printed");
    assertTrue(output.matches("(?ms).+clientId.+clientId-3.+"), "host printed");
    assertTrue(output.matches("(?ms).+host.+host1.+"), "host printed");
    assertTrue(output.matches("(?ms).+host.+host2.+"), "host printed");
    assertTrue(output.matches("(?ms).+host.+host3.+"), "host printed");
    assertTrue(output.matches("(?ms).+groupInstanceId.+instance-1.+"), "group instance id printed");
    assertTrue(output.matches("(?ms).+groupInstanceId.+instance-2.+"), "group instance id printed");
    assertTrue(output.matches("(?ms).+groupInstanceId.+instance-3.+"), "group instance id printed");
    assertTrue(output.matches("(?ms).+Topic Size.+100.+Byte.+"), "statistics");
    assertTrue(output.matches("(?ms).+Partition Count.+1.+"), "statistics");
    assertTrue(output.matches("(?ms).+Partition Size Average.+100.+Byte.+"), "statistics");
    assertTrue(output.matches("(?ms).+Replica Count.+1.+"), "statistics");
    assertTrue(output.matches("(?ms).+Replica Size Average.+100.+Byte.+"), "statistics");
    assertTrue(output.matches("(?ms).+Partition.+0.+"), "partition info printed");
    assertTrue(output.matches("(?ms).+size.+100.+Byte.+"), "statistics");
    assertTrue(output.matches("(?ms).+offset range.+0.+100.+"), "partition offset printed");
    assertTrue(output.matches("(?ms).+replica on broker.+55.+"), "broker id printed");
    assertTrue(output.matches("(?ms).+size.+100.+Byte.+"), "replica size printed");
    assertTrue(output.matches("(?ms).+\\[.*leader.*].+"), "descriptor printed");
    assertTrue(output.matches("(?ms).+\\[.*lagged.+size.+15.+Byte.*].+"), "descriptor printed");
    assertTrue(output.matches("(?ms).+\\[.*non-synced.*].+"), "descriptor printed");
    assertTrue(output.matches("(?ms).+\\[.*online.*].+"), "descriptor printed");
    assertTrue(output.matches("(?ms).+\\[.*future.*].+"), "descriptor printed");
    assertTrue(output.matches("(?ms).+/tmp/path0.+"), "path printed");
  }

  @Test
  void somePartitionsHaveNoLeader() {
    String topicName0 = "poor_topic0";
    String topicName1 = "poor_topic1";
    var payload0 =
        Map.of(
            new TopicPartition(topicName0, 0),
            List.of(new Replica(1000, 0, 0, false, false, false, false, true, "?")));
    var payload1 =
        Map.of(
            new TopicPartition(topicName1, 0),
            List.of(
                new Replica(1000, 0, 0, false, false, false, false, true, "?"),
                new Replica(1001, 0, 0, false, false, false, false, false, "?"),
                new Replica(1002, 0, 0, false, false, false, false, false, "?")));
    Admin mock = Mockito.mock(Admin.class);
    Mockito.when(mock.replicas(Set.of(topicName0))).thenReturn(payload0);
    Mockito.when(mock.replicas(Set.of(topicName1))).thenReturn(payload1);

    assertThrows(
        IllegalStateException.class, () -> TopicExplorer.execute(mock, Set.of(topicName0)));
    assertThrows(
        IllegalStateException.class, () -> TopicExplorer.execute(mock, Set.of(topicName1)));
  }
}
