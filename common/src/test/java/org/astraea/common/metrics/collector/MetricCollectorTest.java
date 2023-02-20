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

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.platform.HostMetrics;
import org.astraea.common.metrics.platform.JvmMemory;
import org.astraea.common.metrics.platform.OperatingSystemInfo;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

class MetricCollectorTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  private static final MetricSensor MEMORY_METRIC_SENSOR =
      (client, ignored) -> List.of(HostMetrics.jvmMemory(client));
  private static final MetricSensor OS_METRIC_SENSOR =
      (client, ignored) -> List.of(HostMetrics.operatingSystem(client));

  @Test
  void testAddSensor() {
    try (var collector = MetricCollector.local().addMetricSensor(MEMORY_METRIC_SENSOR).build()) {
      Assertions.assertEquals(1, collector.metricSensors().size());
      Assertions.assertTrue(collector.metricSensors().contains(MEMORY_METRIC_SENSOR));
    }
  }

  @Test
  void registerJmx() {
    var socket =
        InetSocketAddress.createUnresolved(
            SERVICE.jmxServiceURL().getHost(), SERVICE.jmxServiceURL().getPort());
    var builder = MetricCollector.local().registerJmx(1, socket).registerJmx(-1, socket);

    Assertions.assertThrows(
        IllegalArgumentException.class, builder::build, "The id -1 is already registered");

    try (var collector = MetricCollector.local().registerJmx(1, socket).build()) {
      Assertions.assertEquals(2, collector.listIdentities().size());
      Assertions.assertTrue(collector.listIdentities().contains(-1));
      Assertions.assertTrue(collector.listIdentities().contains(1));
    }
  }

  @Test
  void testListMetricTypes() {
    var sample = Duration.ofMillis(100);
    try (var collector =
        MetricCollector.local()
            .addMetricSensor(MEMORY_METRIC_SENSOR)
            .addMetricSensor(OS_METRIC_SENSOR)
            .interval(sample)
            .build()) {

      Utils.sleep(sample);

      Assertions.assertEquals(
          Set.of(JvmMemory.class, OperatingSystemInfo.class), collector.listMetricTypes());
    }
  }

  @Test
  void clusterBean() {
    var sample = Duration.ofMillis(200);
    try (var collector =
        MetricCollector.local()
            .addMetricSensor(MEMORY_METRIC_SENSOR)
            .addMetricSensor(OS_METRIC_SENSOR)
            .interval(sample)
            .build()) {

      Utils.sleep(sample);
      Utils.sleep(sample);

      ClusterBean clusterBean = collector.clusterBean();

      Assertions.assertEquals(1, clusterBean.all().keySet().size());
      Assertions.assertTrue(
          clusterBean.all().get(-1).stream().anyMatch(x -> x instanceof JvmMemory));
      Assertions.assertTrue(
          clusterBean.all().get(-1).stream().anyMatch(x -> x instanceof OperatingSystemInfo));
    }
  }

  @Test
  void metrics() {
    var sample = Duration.ofSeconds(2);
    try (var collector =
        MetricCollector.local()
            .addMetricSensor(MEMORY_METRIC_SENSOR, (id, err) -> Assertions.fail(err.getMessage()))
            .addMetricSensor(OS_METRIC_SENSOR, (id, err) -> Assertions.fail(err.getMessage()))
            .interval(sample)
            .build()) {

      Utils.sleep(Duration.ofMillis(300));
      var untilNow = System.currentTimeMillis();

      Supplier<List<JvmMemory>> memory =
          () -> collector.metrics(JvmMemory.class).collect(Collectors.toList());
      Supplier<List<OperatingSystemInfo>> os =
          () -> collector.metrics(OperatingSystemInfo.class).collect(Collectors.toList());

      Assertions.assertEquals(1, memory.get().size());
      Assertions.assertTrue(
          memory.get().get(0).createdTimestamp() < untilNow,
          "Sampled before the next interval: "
              + memory.get().get(0).createdTimestamp()
              + " < "
              + untilNow);

      Assertions.assertEquals(1, os.get().size());
      Assertions.assertTrue(
          os.get().get(0).createdTimestamp() < untilNow,
          "Sampled before the next interval: "
              + os.get().get(0).createdTimestamp()
              + " < "
              + untilNow);

      // memory and os
      Assertions.assertEquals(2, collector.metrics().count());
      Assertions.assertEquals(2, collector.size());
    }
  }

  @Test
  void close() {
    List<MBeanClient> clients = new ArrayList<>();
    List<ScheduledExecutorService> services = new ArrayList<>();
    try (var ignore0 = Mockito.mockStatic(MBeanClient.class, sniff("jndi", clients))) {
      try (var ignore1 =
          Mockito.mockStatic(Executors.class, sniff("newScheduledThreadPool", services))) {
        var socket =
            InetSocketAddress.createUnresolved(
                SERVICE.jmxServiceURL().getHost(), SERVICE.jmxServiceURL().getPort());
        var collector =
            MetricCollector.local()
                .registerJmx(0, socket)
                .registerJmx(1, socket)
                .registerJmx(2, socket)
                .addMetricSensor(MEMORY_METRIC_SENSOR)
                .addMetricSensor(MEMORY_METRIC_SENSOR)
                .addMetricSensor(MEMORY_METRIC_SENSOR)
                .build();

        // client & service are created
        Assertions.assertEquals(3, clients.size(), "MBeanClient has been mocked");
        Assertions.assertEquals(1, services.size(), "Executor has been mocked");

        // close it
        collector.close();

        // client & service are closed
        Mockito.verify(clients.get(0), Mockito.times(1)).close();
        Mockito.verify(clients.get(1), Mockito.times(1)).close();
        Mockito.verify(clients.get(2), Mockito.times(1)).close();
        Assertions.assertTrue(services.get(0).isShutdown());
      }
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
        MetricCollector.local()
            .addMetricSensor(
                noSuchMetricSensor,
                (id, ex) -> {
                  Assertions.assertEquals(-1, id);
                  Assertions.assertInstanceOf(NoSuchElementException.class, ex);
                  called.set(true);
                })
            .interval(Duration.ofMillis(100))
            .build()) {

      Utils.sleep(Duration.ofMillis(300));
      Assertions.assertTrue(called.get(), "The error was triggered");
    }
  }

  @Test
  void testCleaner() {
    try (var collector =
        MetricCollector.local()
            .expiration(Duration.ofMillis(2000))
            .cleanerInterval(Duration.ofMillis(50))
            .interval(Duration.ofMillis(100))
            .addMetricSensor(MEMORY_METRIC_SENSOR)
            .build()) {

      Utils.sleep(Duration.ofMillis(1500));
      var beforeCleaning = collector.metrics(JvmMemory.class).collect(Collectors.toList());
      Assertions.assertFalse(beforeCleaning.isEmpty(), "There are some metrics");
      Utils.sleep(Duration.ofMillis(1500));
      var afterCleaning = collector.metrics(JvmMemory.class).collect(Collectors.toList());

      Assertions.assertTrue(
          afterCleaning.get(0).createdTimestamp() != beforeCleaning.get(0).createdTimestamp(),
          "different old metric: "
              + afterCleaning.get(0).createdTimestamp()
              + " != "
              + beforeCleaning.get(0).createdTimestamp());
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> Answer<T> sniff(String functionName, Collection<T> collector) {
    return (invocation) -> {
      if (invocation.getMethod().getName().equals(functionName)) {
        Object o = Mockito.spy(invocation.callRealMethod());
        collector.add((T) o);
        return (T) o;
      } else {
        return (T) invocation.callRealMethod();
      }
    };
  }
}
