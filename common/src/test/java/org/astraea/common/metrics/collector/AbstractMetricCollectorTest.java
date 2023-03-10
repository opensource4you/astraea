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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.astraea.common.metrics.platform.HostMetrics;
import org.astraea.common.metrics.platform.JvmMemory;
import org.astraea.common.metrics.platform.OperatingSystemInfo;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public abstract class AbstractMetricCollectorTest {
  protected static final Service SERVICE = Service.builder().numberOfBrokers(1).build();

  protected abstract MetricCollector collector(
      Map<MetricSensor, BiConsumer<Integer, Exception>> sensors);

  private static final MetricSensor MEMORY_METRIC_SENSOR =
      (client, ignored) -> List.of(HostMetrics.jvmMemory(client));
  private static final MetricSensor OS_METRIC_SENSOR =
      (client, ignored) -> List.of(HostMetrics.operatingSystem(client));
  private static final MetricSensor BYTE_IN_SENSOR =
      (client, ignore) -> List.of(ServerMetrics.BrokerTopic.BYTES_IN_PER_SEC.fetch(client));

  @Test
  void testListIdentity() {
    var sample = Duration.ofSeconds(5);
    try (var collector =
        collector(
            Map.of(
                MEMORY_METRIC_SENSOR,
                (i, e) -> {},
                OS_METRIC_SENSOR,
                (i, e) -> {},
                BYTE_IN_SENSOR,
                (i, e) -> {}))) {
      Utils.sleep(sample);
      var ids = new HashSet<>(SERVICE.dataFolders().keySet());
      ids.add(-1);
      Assertions.assertEquals(ids, collector.listIdentities());
    }
  }

  @Test
  void testListMetricTypes() {
    var sample = Duration.ofSeconds(5);
    try (var collector =
        collector(
            Map.of(
                MEMORY_METRIC_SENSOR,
                (i, e) -> {},
                OS_METRIC_SENSOR,
                (i, e) -> {},
                BYTE_IN_SENSOR,
                (i, e) -> {}))) {
      Utils.sleep(sample);

      Assertions.assertEquals(
          Set.of(JvmMemory.class, OperatingSystemInfo.class, ServerMetrics.BrokerTopic.Meter.class),
          collector.listMetricTypes());
    }
  }

  @Test
  void clusterBean() {
    var sample = Duration.ofSeconds(5);
    try (var collector =
        collector(
            Map.of(
                MEMORY_METRIC_SENSOR,
                (i, e) -> {},
                OS_METRIC_SENSOR,
                (i, e) -> {},
                BYTE_IN_SENSOR,
                (i, e) -> {}))) {
      Utils.sleep(sample);
      Utils.sleep(sample);

      ClusterBean clusterBean = collector.clusterBean();

      // local metric and remote jmx metric
      Assertions.assertEquals(2, clusterBean.all().keySet().size());
      Assertions.assertTrue(
          clusterBean.all().get(-1).stream().anyMatch(x -> x instanceof JvmMemory));
      Assertions.assertTrue(
          clusterBean.all().get(-1).stream().anyMatch(x -> x instanceof OperatingSystemInfo));

      var id = SERVICE.dataFolders().keySet().stream().findAny();
      Assertions.assertTrue(id.isPresent());
      Assertions.assertTrue(
          clusterBean.all().get(id.get()).stream()
              .anyMatch(x -> x instanceof ServerMetrics.BrokerTopic.Meter));
    }
  }

  @Test
  void metrics() {
    try (var collector =
        collector(
            Map.of(
                MEMORY_METRIC_SENSOR,
                (i, e) -> {},
                OS_METRIC_SENSOR,
                (i, e) -> {},
                BYTE_IN_SENSOR,
                (i, e) -> {}))) {
      Utils.sleep(Duration.ofSeconds(5));

      Supplier<List<JvmMemory>> memory =
          () -> collector.metrics(JvmMemory.class).collect(Collectors.toList());
      Supplier<List<OperatingSystemInfo>> os =
          () -> collector.metrics(OperatingSystemInfo.class).collect(Collectors.toList());
      Supplier<List<ServerMetrics.BrokerTopic.Meter>> byteIn =
          () ->
              collector
                  .metrics(ServerMetrics.BrokerTopic.Meter.class)
                  .collect(Collectors.toUnmodifiableList());

      Assertions.assertEquals(2, memory.get().size());
      Assertions.assertEquals(2, os.get().size());
      // Broker is created on the same jvm. So the local jmx could get kafka metric.
      Assertions.assertEquals(2, byteIn.get().size());

      // memory(2) and os(2) and byte in(2)
      Assertions.assertEquals(6, collector.metrics().count());
      Assertions.assertEquals(6, collector.size());
    }
  }

  @Test
  void testSensorErrorHandling() {
    var called = new AtomicBoolean();
    MetricSensor noSuchMetricSensor =
        (client, ignored) -> {
          BeanObject beanObject =
              client.bean(
                  BeanQuery.builder().domainName("no.such.metric").property("k", "v").build());
          return List.of(() -> beanObject);
        };
    try (var collector =
        collector(
            Map.of(
                noSuchMetricSensor,
                (id, ex) -> {
                  Assertions.assertInstanceOf(NoSuchElementException.class, ex);
                  called.set(true);
                }))) {
      Utils.sleep(Duration.ofSeconds(6));
      Assertions.assertEquals(0, collector.clusterBean().all().size());
      Assertions.assertTrue(called.get(), "The error was triggered");
    }
  }

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }
}
