package org.astraea.metrics;

import java.time.Duration;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.astraea.Utils;
import org.astraea.metrics.java.JvmMemory;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class BeanCollectorTest extends RequireBrokerCluster {

  private static MBeanClient mock(String host, int port) {
    var client = Mockito.mock(MBeanClient.class);
    Mockito.when(client.host()).thenReturn(host);
    Mockito.when(client.port()).thenReturn(port);
    return client;
  }

  @Test
  void testMultiplesNodes() {
    var bean = Mockito.mock(HasBeanObject.class);
    try (var collector = BeanCollector.builder().build()) {
      collector
          .register()
          .host("aa")
          .port(22222)
          .clientSupplier(() -> mock("aa", 22222))
          .metricsGetter(client -> bean)
          .build();

      Assertions.assertEquals(1, collector.nodes().size());

      collector
          .register()
          .host("aa")
          .port(33333)
          .clientSupplier(() -> mock("aa", 33333))
          .metricsGetter(client -> bean)
          .build();

      Assertions.assertEquals(2, collector.nodes().size());

      // add it again
      collector
          .register()
          .host("aa")
          .port(22222)
          .clientSupplier(() -> mock("aa", 22222))
          .metricsGetter(client -> bean)
          .build();
      Assertions.assertEquals(2, collector.nodes().size());
    }
  }

  @Test
  void testNumberOfThreads() {
    var numberOfThreads = 3;
    var client = mock("host", 11111);
    var bean = Mockito.mock(HasBeanObject.class);
    try (var collector = BeanCollector.builder().numberOfThreads(numberOfThreads).build()) {
      var threads = new ConcurrentSkipListSet<String>();
      collector
          .register()
          .host("this")
          .port(1234)
          .clientSupplier(() -> client)
          .metricsGetter(
              ignore -> {
                threads.add(Thread.currentThread().getName());
                return bean;
              })
          .build();
      Utils.waitFor(() -> threads.size() == numberOfThreads);
    }
  }

  @Test
  void testNumberOfObjectsPerNode() {
    var numberOfObjectsPerNode = 10;
    var client = mock("host", 11111);
    var bean = Mockito.mock(HasBeanObject.class);
    try (var collector =
        BeanCollector.builder()
            .interval(Duration.ofMillis(100))
            .numberOfObjectsPerNode(numberOfObjectsPerNode)
            .build()) {
      collector
          .register()
          .host("this")
          .port(1234)
          .clientSupplier(() -> client)
          .metricsGetter(ignore -> bean)
          .build();
      Utils.waitFor(() -> collector.numberOfObjects() >= numberOfObjectsPerNode);
    }
  }

  @Test
  void testInterval() throws Exception {
    var client = mock("host", 11111);
    var bean = Mockito.mock(HasBeanObject.class);
    try (var collector = BeanCollector.builder().interval(Duration.ofSeconds(1000)).build()) {
      collector
          .register()
          .host("this")
          .port(1234)
          .clientSupplier(() -> client)
          .metricsGetter(ignore -> bean)
          .build();

      collector.requestToUpdate();
      TimeUnit.SECONDS.sleep(3);
      var current = collector.numberOfObjects();

      collector.requestToUpdate();
      Utils.waitFor(() -> collector.numberOfObjects() > current);
    }
  }

  @Test
  void testReleaseClient() {
    var client = mock("host", 11111);
    var bean = Mockito.mock(HasBeanObject.class);
    try (var collector = BeanCollector.builder().build()) {
      var object1 =
          collector
              .register()
              .host("this")
              .port(1234)
              .clientSupplier(() -> client)
              .metricsGetter(ignore -> bean)
              .build();

      Assertions.assertEquals(1, collector.numberOfGetters());

      var object2 =
          collector
              .register()
              .host("this")
              .port(1234)
              .clientSupplier(() -> client)
              .metricsGetter(ignore -> bean)
              .build();

      Assertions.assertEquals(2, collector.numberOfGetters());

      object1.removeGetter();
      Assertions.assertEquals(1, collector.numberOfGetters());

      object2.removeGetter();
      Assertions.assertEquals(0, collector.numberOfGetters());
    }
  }

  @Test
  void testAddNode() {
    try (var collector = BeanCollector.builder().build()) {
      var releaseGetter =
          collector
              .register()
              .host(jmxServiceURL().getHost())
              .port(jmxServiceURL().getPort())
              .metricsGetter(KafkaMetrics.Host::jvmMemory)
              .build();
      Utils.waitFor(() -> collector.numberOfObjects() > 0);
      collector
          .objects()
          .values()
          .forEach(
              all -> all.forEach(object -> Assertions.assertTrue(object instanceof JvmMemory)));

      collector
          .nodes()
          .forEach(
              node ->
                  Assertions.assertNotEquals(
                      0,
                      collector.objects(node.host(), node.port()).size(),
                      String.format(
                          "all nodes: %s current: %s",
                          collector.nodes().stream()
                              .map(n -> n.host() + ":" + n.port())
                              .collect(Collectors.joining(",")),
                          node.host() + ":" + node.port())));

      collector
          .nodes()
          .forEach(node -> Assertions.assertEquals(node.host(), jmxServiceURL().getHost()));

      collector
          .nodes()
          .forEach(node -> Assertions.assertEquals(node.port(), jmxServiceURL().getPort()));

      releaseGetter.removeGetter();
      Assertions.assertEquals(0, collector.numberOfGetters());
    }
  }

  @Test
  void testAddDuplicateClient() {
    try (var collector = BeanCollector.builder().build()) {
      collector
          .register()
          .host(jmxServiceURL().getHost())
          .port(jmxServiceURL().getPort())
          .metricsGetter(KafkaMetrics.Host::jvmMemory)
          .build();
      collector
          .register()
          .host(jmxServiceURL().getHost())
          .port(jmxServiceURL().getPort())
          .clientSupplier(
              () -> {
                throw new RuntimeException("the client should be existent!!!");
              })
          .metricsGetter(KafkaMetrics.Host::jvmMemory)
          .build();
      Assertions.assertEquals(1, collector.nodes().size());
    }
  }
}
