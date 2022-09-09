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
package org.astraea.app.performance;

import com.beust.jcommander.ParameterException;
import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.Replica;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.argument.Argument;
import org.astraea.app.common.DataRate;
import org.astraea.app.common.Utils;
import org.astraea.app.consumer.Isolation;
import org.astraea.app.producer.Acks;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PerformanceTest extends RequireBrokerCluster {

  @Test
  void testTransactionalProducer() {
    var topic = "testTransactionalProducer";
    String[] arguments1 = {
      "--bootstrap.servers", bootstrapServers(), "--topics", topic, "--transaction.size", "2"
    };
    var argument = Argument.parse(new Performance.Argument(), arguments1);
    try (var producer = argument.createProducer()) {
      Assertions.assertTrue(producer.transactional());
    }
  }

  @Test
  void testProducerExecutor() throws InterruptedException {
    var topic = "testProducerExecutor";
    String[] arguments1 = {
      "--bootstrap.servers", bootstrapServers(), "--topics", topic, "--compression", "gzip"
    };
    var latch = new CountDownLatch(1);
    var argument = Argument.parse(new Performance.Argument(), arguments1);
    try (var producer = argument.createProducer()) {
      Assertions.assertFalse(producer.transactional());
    }
  }

  @Test
  void testTransactionSet() {
    var argument = new Performance.Argument();
    Assertions.assertEquals(Isolation.READ_UNCOMMITTED, argument.isolation());
    argument.transactionSize = 1;
    Assertions.assertEquals(Isolation.READ_UNCOMMITTED, argument.isolation());
    argument.transactionSize = 3;
    Assertions.assertEquals(Isolation.READ_COMMITTED, argument.isolation());
  }

  @Test
  void testArgument() {
    String[] arguments1 = {
      "--bootstrap.servers",
      "localhost:9092",
      "--topics",
      "not-empty",
      "--partitions",
      "10",
      "--replicas",
      "3",
      "--producers",
      "1",
      "--consumers",
      "1",
      "--run.until",
      "1000records",
      "--value.size",
      "10KiB",
      "--value.distribution",
      "uniform",
      "--partitioner",
      "org.astraea.partitioner.smooth.SmoothWeightPartitioner",
      "--compression",
      "lz4",
      "--key.size",
      "4Byte",
      "--key.distribution",
      "zipfian",
      "--specify.brokers",
      "1",
      "--throughput",
      "100MB/m",
      "--configs",
      "key=value"
    };

    var arg = Argument.parse(new Performance.Argument(), arguments1);
    Assertions.assertEquals("value", arg.configs().get("key"));
    Assertions.assertEquals(DataRate.MB.of(100).perMinute(), arg.throughput);

    String[] arguments2 = {"--bootstrap.servers", "localhost:9092", "--topics", ""};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments2));

    String[] arguments3 = {"--bootstrap.servers", "localhost:9092", "--replicas", "0"};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments3));

    String[] arguments4 = {"--bootstrap.servers", "localhost:9092", "--partitions", "0"};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments4));

    String[] arguments5 = {"--bootstrap.servers", "localhost:9092", "--producers", "0"};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments5));

    String[] arguments6 = {"--bootstrap.servers", "localhost:9092", "--consumers", "0"};
    Assertions.assertDoesNotThrow(() -> Argument.parse(new Performance.Argument(), arguments6));

    String[] arguments7 = {"--bootstrap.servers", "localhost:9092", "--run.until", "1"};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments7));

    String[] arguments8 = {"--bootstrap.servers", "localhost:9092", "--key.distribution", "f"};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments8));

    String[] arguments9 = {"--bootstrap.servers", "localhost:9092", "--key.size", "1"};
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Argument.parse(new Performance.Argument(), arguments9));

    String[] arguments10 = {"--bootstrap.servers", "localhost:9092", "--value.distribution", "u"};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments10));

    String[] arguments11 = {"--bootstrap.servers", "localhost:9092", "--value.size", "2"};
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Argument.parse(new Performance.Argument(), arguments11));

    String[] arguments12 = {"--bootstrap.servers", "localhost:9092", "--partitioner", ""};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments12));

    String[] arguments13 = {"--bootstrap.servers", "localhost:9092", "--compression", ""};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments13));

    String[] arguments14 = {"--bootstrap.servers", "localhost:9092", "--specify.brokers", ""};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments14));

    String[] arguments15 = {"--bootstrap.servers", "localhost:9092", "--topics", "test1,,test2"};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments15));

    String[] arguments21 = {
      "--bootstrap.servers", "localhost:9092", "--partitions", "0,10,10",
    };
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments21));

    String[] arguments22 = {
      "--bootstrap.servers", "localhost:9092", "--replicas", "0,2,1",
    };
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments22));
  }

  @Test
  void testChaosFrequency() {
    var args =
        Argument.parse(
            new Performance.Argument(),
            new String[] {"--bootstrap.servers", "localhost:9092", "--chaos.frequency", "10s"});
    Assertions.assertEquals(Duration.ofSeconds(10), args.chaosDuration);
  }

  @Test
  void testPartitionSupplier() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(6).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(2));
      var args =
          Argument.parse(
              new Performance.Argument(),
              new String[] {
                "--bootstrap.servers",
                bootstrapServers(),
                "--topics",
                topicName,
                "--specify.brokers",
                "1"
              });
      var expectedLeaders =
          admin.replicas(Set.of(topicName)).values().stream()
              .flatMap(Collection::stream)
              .filter(Replica::isLeader)
              .filter(r -> r.nodeInfo().id() == 1)
              .map(ReplicaInfo::topicPartition)
              .collect(Collectors.toUnmodifiableSet());

      // assert there are 3 brokers, the 6 partitions are divided
      Assertions.assertEquals(3, brokerIds().size());
      Assertions.assertEquals(2, expectedLeaders.size());

      var selector = args.topicPartitionSelector();
      var actual =
          IntStream.range(0, 1000)
              .mapToObj(ignored -> selector.get())
              .collect(Collectors.toUnmodifiableSet());

      Assertions.assertEquals(expectedLeaders, actual);

      // test multiple topics
      var topicName2 = Utils.randomString(10);
      admin.creator().topic(topicName2).numberOfPartitions(3).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(2));
      args =
          Argument.parse(
              new Performance.Argument(),
              new String[] {
                "--bootstrap.servers",
                bootstrapServers(),
                "--topics",
                topicName + "," + topicName2,
                "--specify.brokers",
                "1"
              });

      var expected2 =
          admin.replicas(Set.of(topicName, topicName2)).values().stream()
              .flatMap(Collection::stream)
              .filter(ReplicaInfo::isLeader)
              .filter(replica -> replica.nodeInfo().id() == 1)
              .map(ReplicaInfo::topicPartition)
              .collect(Collectors.toSet());
      var selector2 = args.topicPartitionSelector();
      var actual2 =
          IntStream.range(0, 10000)
              .mapToObj(ignored -> selector2.get())
              .collect(Collectors.toUnmodifiableSet());
      Assertions.assertEquals(expected2, actual2);

      // no specify broker
      Assertions.assertEquals(
          -1,
          Argument.parse(
                  new Performance.Argument(),
                  new String[] {"--bootstrap.servers", bootstrapServers(), "--topics", topicName})
              .topicPartitionSelector()
              .get()
              .partition());

      // Test no partition in specified broker
      var topicName3 = Utils.randomString(10);
      admin.creator().topic(topicName3).numberOfPartitions(1).create();
      Utils.sleep(Duration.ofSeconds(2));
      args =
          Argument.parse(
              new Performance.Argument(),
              new String[] {
                "--bootstrap.servers",
                bootstrapServers(),
                "--topics",
                topicName3,
                "--specify.brokers",
                "4"
              });
      Assertions.assertThrows(IllegalArgumentException.class, args::topicPartitionSelector);

      // test specify partitions
      var topicName4 = Utils.randomString();
      var topicName5 = Utils.randomString();
      admin.creator().topic(topicName4).numberOfPartitions(3).create();
      admin.creator().topic(topicName5).numberOfPartitions(3).create();
      Utils.sleep(Duration.ofSeconds(2));
      var targets =
          Set.of(
              TopicPartition.of(topicName4, 0),
              TopicPartition.of(topicName4, 1),
              TopicPartition.of(topicName5, 2));
      var arguments =
          Argument.parse(
              new Performance.Argument(),
              new String[] {
                "--bootstrap.servers",
                bootstrapServers(),
                "--specify.partitions",
                targets.stream().map(TopicPartition::toString).collect(Collectors.joining(","))
              });
      var selector3 = arguments.topicPartitionSelector();

      Assertions.assertEquals(targets, Set.copyOf(arguments.specifyPartitions));
      Assertions.assertEquals(
          targets,
          IntStream.range(0, 10000)
              .mapToObj(ignore -> selector3.get())
              .collect(Collectors.toUnmodifiableSet()));
    }
  }

  @Test
  void testLastOffsets() {
    var partitionCount = 40;
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      // large partitions
      admin.creator().topic(topicName).numberOfPartitions(partitionCount).create();
      Utils.sleep(Duration.ofSeconds(2));
      var args =
          Argument.parse(
              new Performance.Argument(),
              new String[] {"--bootstrap.servers", bootstrapServers(), "--topics", topicName});
      try (var producer = args.createProducer()) {
        IntStream.range(0, 250)
            .forEach(
                i -> producer.sender().topic(topicName).key(String.valueOf(i).getBytes()).run());
      }
      Assertions.assertEquals(partitionCount, args.lastOffsets().size());
      System.out.println(args.lastOffsets());
      args.lastOffsets().values().forEach(v -> Assertions.assertNotEquals(0, v));
    }
  }

  @Test
  void testInitTopic() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(3).create();
      Utils.sleep(Duration.ofSeconds(2));
      var args =
          Argument.parse(
              new Performance.Argument(),
              new String[] {
                "--bootstrap.servers",
                bootstrapServers(),
                "--topics",
                topicName,
                "--partitions",
                "3",
                "--replicas",
                "1"
              });
      // they should all pass since the passed arguments are equal to existent topic
      args.initTopics();
      args.initTopics();
      args.initTopics();
    }
  }

  @Test
  public void testCustomCreateMode() {
    try (var admin = Admin.of(bootstrapServers())) {
      var args =
          Argument.parse(
              new Performance.Argument(),
              new String[] {
                "--bootstrap.servers",
                bootstrapServers(),
                "--topics",
                "test,test1",
                "--partitions",
                "3,5",
                "--replicas",
                "2,1"
              });
      args.initTopics();

      Assertions.assertEquals(3, admin.partitions(Set.of("test")).size());
      Assertions.assertEquals(5, admin.partitions(Set.of("test1")).size());

      admin
          .replicas(Set.of("test"))
          .forEach((topicPartition, replicas) -> Assertions.assertEquals(2, replicas.size()));
      admin
          .replicas(Set.of("test1"))
          .forEach((topicPartition, replicas) -> Assertions.assertEquals(1, replicas.size()));
    }
  }

  @Test
  public void testDefaultCreateMode() {
    try (var admin = Admin.of(bootstrapServers())) {
      var args =
          Argument.parse(
              new Performance.Argument(),
              new String[] {
                "--bootstrap.servers",
                bootstrapServers(),
                "--topics",
                "test2,test3,test4,test5,test6",
                "--partitions",
                "3",
                "--replicas",
                "2"
              });
      args.initTopics();
      Assertions.assertEquals(3, admin.partitions(Set.of("test2")).size());
      Assertions.assertEquals(3, admin.partitions(Set.of("test3")).size());
      Assertions.assertEquals(3, admin.partitions(Set.of("test4")).size());
      Assertions.assertEquals(3, admin.partitions(Set.of("test5")).size());
      Assertions.assertEquals(3, admin.partitions(Set.of("test6")).size());

      admin
          .replicas(Set.of("test2"))
          .forEach((topicPartition, replicas) -> Assertions.assertEquals(2, replicas.size()));
      admin
          .replicas(Set.of("test3"))
          .forEach((topicPartition, replicas) -> Assertions.assertEquals(2, replicas.size()));
      admin
          .replicas(Set.of("test4"))
          .forEach((topicPartition, replicas) -> Assertions.assertEquals(2, replicas.size()));
      admin
          .replicas(Set.of("test5"))
          .forEach((topicPartition, replicas) -> Assertions.assertEquals(2, replicas.size()));
      admin
          .replicas(Set.of("test6"))
          .forEach((topicPartition, replicas) -> Assertions.assertEquals(2, replicas.size()));
    }
  }

  @Test
  public void testTopicPattern() {
    try (var admin = Admin.of(bootstrapServers())) {
      var args =
          Argument.parse(
              new Performance.Argument(),
              new String[] {
                "--bootstrap.servers",
                bootstrapServers(),
                "--topics",
                "test2,test3,test4,test5,test6",
                "--partitions",
                "3,2,1,2,3",
                "--replicas",
                "2,1"
              });
      Assertions.assertThrows(ParameterException.class, () -> args.topicPattern());

      var customArgs =
          Argument.parse(
              new Performance.Argument(),
              new String[] {
                "--bootstrap.servers",
                bootstrapServers(),
                "--topics",
                "test2,test3",
                "--partitions",
                "3,2",
                "--replicas",
                "2,1"
              });
      var customPattern = customArgs.topicPattern();
      var customTopics = customPattern.keySet();
      Assertions.assertEquals(2, customTopics.size());
      Assertions.assertTrue(customTopics.contains("test2"));
      Assertions.assertTrue(customTopics.contains("test3"));
      Assertions.assertTrue(customPattern.get("test2").containsKey(3));
      Assertions.assertTrue(customPattern.get("test2").containsValue((short) 2));
      Assertions.assertTrue(customPattern.get("test3").containsKey(2));
      Assertions.assertTrue(customPattern.get("test3").containsValue((short) 1));

      var defaultArgs =
          Argument.parse(
              new Performance.Argument(),
              new String[] {
                "--bootstrap.servers",
                bootstrapServers(),
                "--topics",
                "test2,test3",
                "--partitions",
                "3",
                "--replicas",
                "2"
              });
      var defaultPattern = defaultArgs.topicPattern();
      var defaultTopics = defaultPattern.keySet();
      Assertions.assertEquals(2, defaultTopics.size());
      Assertions.assertTrue(defaultTopics.contains("test2"));
      Assertions.assertTrue(defaultTopics.contains("test3"));
      Assertions.assertTrue(defaultPattern.get("test2").containsKey(3));
      Assertions.assertTrue(defaultPattern.get("test2").containsValue((short) 2));
      Assertions.assertTrue(defaultPattern.get("test3").containsKey(3));
      Assertions.assertTrue(defaultPattern.get("test3").containsValue((short) 2));
    }
  }

  @Test
  void testAcks() {
    Stream.of(Acks.values())
        .forEach(
            ack -> {
              var arg =
                  Argument.parse(
                      new Performance.Argument(),
                      new String[] {
                        "--bootstrap.servers", bootstrapServers(), "--acks", ack.alias()
                      });
              Assertions.assertEquals(ack, arg.acks);
            });
  }
}
