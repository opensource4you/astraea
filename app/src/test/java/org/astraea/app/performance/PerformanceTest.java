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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import org.astraea.app.argument.Argument;
import org.astraea.app.consumer.Isolation;
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
    var latch = new CountDownLatch(1);
    BiConsumer<Long, Integer> observer = (x, y) -> latch.countDown();
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
    BiConsumer<Long, Integer> observer = (x, y) -> latch.countDown();
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
      "--specify.broker",
      "1",
      "--configs",
      "key=value"
    };

    var arg = Argument.parse(new Performance.Argument(), arguments1);
    Assertions.assertEquals("value", arg.configs().get("key"));

    String[] arguments2 = {"--bootstrap.servers", "localhost:9092", "--topic", ""};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments2));

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

    String[] arguments14 = {"--bootstrap.servers", "localhost:9092", "--specify.broker", ""};
    Assertions.assertThrows(
        ParameterException.class, () -> Argument.parse(new Performance.Argument(), arguments14));
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
  void testReplicaCount() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Performance.vaildateReplicas(List.of(10, 0), List.of(1, 1)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Performance.vaildateReplicas(List.of(10, 0), List.of(0, 1)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Performance.vaildateReplicas(List.of(10, 10), List.of(0, 1)));
  }
}
