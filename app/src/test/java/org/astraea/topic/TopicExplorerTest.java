package org.astraea.topic;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.astraea.consumer.Consumer;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

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
                          Consumer.builder()
                              .brokers(bootstrapServers())
                              .topics(topics)
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
  void testExecute() throws InterruptedException {
    // arrange
    var topicName = "TopicExplorerTest-testExecute-Topic";
    var groupName = "TopicExplorerTest-testExecute-ConsumerGroup";
    try (var admin = TopicAdmin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(3).numberOfReplicas((short) 1).create();
      consumer(Set.of(topicName), groupName, "alpha");
      consumer(Set.of(topicName), groupName, "beta");
      consumer(Set.of(topicName), groupName, "gamma");

      TimeUnit.SECONDS.sleep(10); // wait for broker process resource creation

      // act
      var result = TopicExplorer.execute(admin, Set.of(topicName));

      // assert
      assertTrue(
          ChronoUnit.SECONDS.between(result.time, LocalDateTime.now()) < 10,
          "assert the execution timestamp come from a reasonable passed time (10sec before)");
      assertTrue(result.consumerGroupMembers.containsKey(groupName));
      assertEquals(3, result.consumerGroupMembers.get(groupName).size());
      assertTrue(
          result.consumerGroupMembers.get(groupName).stream()
              .anyMatch(x -> x.groupInstanceId().orElseThrow().equals("alpha")));
      assertTrue(
          result.consumerGroupMembers.get(groupName).stream()
              .anyMatch(x -> x.groupInstanceId().orElseThrow().equals("beta")));
      assertTrue(
          result.consumerGroupMembers.get(groupName).stream()
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
            new Member("memberId-1", Optional.of("instance-1"), "clientId-1", "host1"),
            new Member("memberId-2", Optional.of("instance-2"), "clientId-2", "host2"),
            new Member("memberId-3", Optional.of("instance-3"), "clientId-3", "host3"));
    var partitionInfo =
        List.of(
            new TopicExplorer.PartitionInfo(
                new TopicPartition("my-topic", 0),
                0,
                100,
                List.of(
                    new Group(
                        "my-consumer-group-1", OptionalLong.of(50), List.of(groupMembers.get(0)))),
                List.of(new Replica(55, 15, 100, true, false, true, "/tmp/path0"))));
    var result =
        new TopicExplorer.Result(
            now, Map.of("my-topic", partitionInfo), Map.of("my-consumer-group-1", groupMembers));

    // act
    TopicExplorer.TreeOutput.print(result, printStream);

    // assert
    final String output = mockOutput.toString();
    System.out.println(output);
    assertTrue(output.matches("(?ms).+" + now + ".+"), "time printed");
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
    assertTrue(output.matches("(?ms).+Partition.+0.+"), "partition info printed");
    assertTrue(output.matches("(?ms).+offset range.+0.+100.+"), "partition offset printed");
    assertTrue(output.matches("(?ms).+replica on broker.+55.+"), "broker id printed");
    assertTrue(output.matches("(?ms).+size.+100.+Byte.+"), "replica size printed");
    assertTrue(output.matches("(?ms).+[.+leader.+].+"), "descriptor printed");
    assertTrue(output.matches("(?ms).+[.+lagged.+size.+15.+Byte.+].+"), "descriptor printed");
    assertTrue(output.matches("(?ms).+[.+non-synced.+].+"), "descriptor printed");
    assertTrue(output.matches("(?ms).+[.+future.+].+"), "descriptor printed");
    assertTrue(output.matches("(?ms).+/tmp/path0.+"), "path printed");
  }
}
