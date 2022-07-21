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
package org.astraea.app.metrics.collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.common.Utils;
import org.astraea.app.concurrent.Executor;
import org.astraea.app.concurrent.State;
import org.astraea.app.concurrent.ThreadPool;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.KafkaMetrics;
import org.astraea.app.metrics.jmx.BeanObject;
import org.astraea.app.metrics.jmx.MBeanClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class BeanCollectorTest {
  private final MBeanClient mbeanClient = Mockito.mock(MBeanClient.class);
  private final BiFunction<String, Integer, MBeanClient> clientCreator =
      (host, port) -> mbeanClient;

  private static Executor executor(Runnable runnable) {
    return () -> {
      runnable.run();
      return State.RUNNING;
    };
  }

  private static HasBeanObject createBeanObject() {
    var obj = new BeanObject("domain", Map.of(), Map.of());
    return () -> obj;
  }

  @Test
  void testAddress() {
    var collector = BeanCollector.builder().clientCreator(clientCreator).build();
    var receiver =
        collector
            .register()
            .host("unknown")
            .port(100)
            .fetcher(client -> List.of(Mockito.mock(HasBeanObject.class)))
            .build();
    Assertions.assertEquals("unknown", receiver.host());
    Assertions.assertEquals(100, receiver.port());
  }

  @Test
  void theImmutableCurrent() {
    var collector = BeanCollector.builder().clientCreator(clientCreator).build();
    var receivers = receivers(collector);
    receivers.forEach(
        r ->
            Assertions.assertThrows(
                UnsupportedOperationException.class, () -> r.current().add(createBeanObject())));
  }

  @Test
  void testNumberOfObjectsPerNode() {
    var collector =
        BeanCollector.builder()
            .numberOfObjectsPerNode(2)
            .interval(Duration.ofMillis(1))
            .clientCreator(clientCreator)
            .build();

    var receiver =
        collector
            .register()
            .host("unknown")
            .port(100)
            .fetcher(client -> List.of(createBeanObject()))
            .build();

    var c0 = receiver.current();
    Assertions.assertEquals(1, c0.size());
    var firstObject = c0.iterator().next();
    Utils.sleep(Duration.ofSeconds(1));

    var c1 = receiver.current();
    Assertions.assertEquals(2, c1.size());
    var secondObject = c1.stream().filter(o -> o != firstObject).findFirst().get();
    Utils.sleep(Duration.ofSeconds(1));

    var c2 = receiver.current();
    Assertions.assertEquals(2, c2.size());
    // the oldest element should be removed
    Assertions.assertFalse(c2.contains(firstObject));
    Assertions.assertTrue(c2.contains(secondObject));
  }

  @Test
  void testBeanCollectorBuilder() {
    Assertions.assertThrows(
        NullPointerException.class, () -> BeanCollector.builder().interval(null));
    Assertions.assertThrows(
        NullPointerException.class, () -> BeanCollector.builder().clientCreator(null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> BeanCollector.builder().numberOfObjectsPerNode(-1));
  }

  @Test
  void testRegister() {
    var collector = BeanCollector.builder().clientCreator(clientCreator).build();
    Assertions.assertThrows(NullPointerException.class, () -> collector.register().host(null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> collector.register().port(-1));
    Assertions.assertThrows(NullPointerException.class, () -> collector.register().fetcher(null));
  }

  private List<Receiver> receivers(BeanCollector collector) {
    var receivers = new ArrayList<Receiver>();
    Runnable runnable =
        () -> {
          try {
            var receiver =
                collector
                    .register()
                    .host("unknown")
                    .port(100)
                    .fetcher(client -> List.of(createBeanObject()))
                    .build();
            synchronized (receivers) {
              receivers.add(receiver);
            }
          } finally {
            Utils.sleep(Duration.ofSeconds(1));
          }
        };

    try (var pool =
        ThreadPool.builder()
            .executors(
                IntStream.range(0, 3)
                    .mapToObj(i -> executor(runnable))
                    .collect(Collectors.toList()))
            .build()) {
      Utils.sleep(Duration.ofSeconds(1));
    }
    return receivers;
  }

  @Test
  void testCloseReceiver() {
    var collector = BeanCollector.builder().clientCreator(clientCreator).build();

    var receivers = receivers(collector);
    receivers.forEach(Receiver::current);

    Assertions.assertEquals(1, collector.nodes.size());
    Assertions.assertEquals(
        mbeanClient, collector.nodes.entrySet().iterator().next().getValue().mBeanClient);

    receivers.forEach(Receiver::close);
    Assertions.assertEquals(1, collector.nodes.size());
    Assertions.assertNull(collector.nodes.entrySet().iterator().next().getValue().mBeanClient);
  }

  @Test
  void testMultiThreadsUpdate() {

    var collector =
        BeanCollector.builder()
            .interval(Duration.ofSeconds(100))
            .clientCreator(clientCreator)
            .build();

    var receivers = receivers(collector);

    try (var pool =
        ThreadPool.builder()
            .executors(
                receivers.stream()
                    .map(receiver -> executor(receiver::current))
                    .collect(Collectors.toList()))
            .build()) {
      Utils.sleep(Duration.ofSeconds(3));
    }
    receivers.forEach(r -> Assertions.assertEquals(1, r.current().size()));
  }

  @Test
  void testLargeInterval() {
    testInterval(Duration.ofSeconds(100), List.of(1, 1, 1));
  }

  @Test
  void testSmallInterval() {
    testInterval(Duration.ofMillis(100), List.of(1, 2, 3));
  }

  private void testInterval(Duration interval, List<Integer> expectedSizes) {
    var collector = BeanCollector.builder().interval(interval).clientCreator(clientCreator).build();
    var count = new AtomicInteger();
    var receiver =
        collector
            .register()
            .host("unknown")
            .port(100)
            .fetcher(
                client -> {
                  count.incrementAndGet();
                  return List.of(createBeanObject());
                })
            .build();

    for (var expect : expectedSizes) {
      Assertions.assertEquals(expect, receiver.current().size());
      Assertions.assertEquals(expect, count.get());
      Utils.sleep(Duration.ofSeconds(1));
    }
  }

  @Test
  void testMultiplesReceiversData() {
    var collector =
        BeanCollector.builder()
            .interval(Duration.ofSeconds(1))
            .clientCreator(clientCreator)
            .build();

    var receivers =
        IntStream.range(0, 2)
            .mapToObj(
                i -> {
                  var count = new AtomicInteger();
                  return Map.entry(
                      count,
                      collector
                          .register()
                          .host("unknown")
                          .port(100)
                          .fetcher(
                              client -> {
                                count.incrementAndGet();
                                return List.of(createBeanObject());
                              })
                          .build());
                })
            .collect(Collectors.toList());

    Utils.sleep(Duration.ofSeconds(1));
    receivers.forEach(e -> Assertions.assertEquals(0, e.getKey().get()));

    Utils.sleep(Duration.ofSeconds(1));
    receivers.forEach(e -> Assertions.assertEquals(1, e.getValue().current().size()));
    receivers.forEach(e -> Assertions.assertEquals(1, e.getKey().get()));
  }

  @Test
  void testCreatedTimestamp() {
    var collector =
        BeanCollector.builder()
            .interval(Duration.ofSeconds(1))
            .clientCreator(clientCreator)
            .build();
    var obj = new BeanObject("domain", Map.of(), Map.of());
    try (var receiver =
        collector
            .register()
            .host("unknown")
            .port(100)
            .fetcher(client -> List.of(() -> obj))
            .build()) {
      // wait for updating cache
      Utils.sleep(Duration.ofSeconds(1));
      var objs = receiver.current();
      Assertions.assertEquals(1, objs.size());
      Assertions.assertEquals(obj.createdTimestamp(), objs.iterator().next().createdTimestamp());
    }
  }

  @Test
  void testLocal() {
    var collector =
        BeanCollector.builder()
            .interval(Duration.ofSeconds(1))
            .clientCreator(clientCreator)
            .build();
    try (var receiver =
        collector
            .register()
            .local()
            .fetcher(client -> List.of(KafkaMetrics.Host.jvmMemory(client)))
            .build()) {

      // wait for updating cache
      Utils.sleep(Duration.ofSeconds(1));
      Assertions.assertNotEquals(0, receiver.current().size());
    }
  }
}
