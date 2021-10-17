package org.astraea.partitioner.nodeLoadMetric;

import static java.util.Arrays.asList;
import static org.astraea.partitioner.partitionerFactory.LinkPartitioner.getFactory;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.partitioner.partitionerFactory.LinkPartitioner;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

public class SmoothPartitionerFactoryTest {
  private String jmxAddresses;

  private static final Node[] NODES =
      new Node[] {
        new Node(0, "localhost", 99), new Node(1, "localhost", 100), new Node(12, "localhost", 101)
      };
  private static final String TOPIC = "test";
  // Intentionally make the partition list not in partition order to test the edge cases.
  private static final List<PartitionInfo> PARTITIONS =
      asList(
          new PartitionInfo(TOPIC, 1, null, NODES, NODES),
          new PartitionInfo(TOPIC, 2, NODES[1], NODES, NODES),
          new PartitionInfo(TOPIC, 0, NODES[0], NODES, NODES));

  @BeforeEach
  void reset() {
    jmxAddresses = "0.0.0.0,1.1.1.1,2.2.2.2";
  }

  @Test
  void testSingletonByProducer() {
    try (MockedConstruction mocked =
        mockConstruction(LinkPartitioner.ThreadSafeSmoothPartitioner.class)) {

      var props =
          Map.<String, Object>of(
              ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
              "localhost:11111",
              ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              ByteArraySerializer.class.getName(),
              ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              ByteArraySerializer.class.getName(),
              ProducerConfig.PARTITIONER_CLASS_CONFIG,
              LinkPartitioner.class.getName(),
              "jmx_servers",
              jmxAddresses);
      // create multiples partitioners
      var producers =
          IntStream.range(0, 10)
              .mapToObj(i -> new KafkaProducer<byte[], byte[]>(props))
              .collect(Collectors.toList());

      Assertions.assertEquals(getFactory().getSmoothPartitionerMap().size(), 1);
      Partitioner partitioner = getFactory().getSmoothPartitionerMap().get(props);

      // ThreadSafePartitioner is created only once
      verify(partitioner).configure(any());
      verify(partitioner, never()).partition(anyString(), any(), any(), any(), any(), any());
      verify(partitioner, never()).close();

      producers.forEach(Producer::close);
      // ThreadSafePartitioner is closed only once
      verify(partitioner).configure(any());
      verify(partitioner, never()).partition(anyString(), any(), any(), any(), any(), any());
      verify(partitioner).close();
    }
  }

  @Test
  void testThrowException() {
    var configs =
        Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:11111", "jmx_server", jmxAddresses);

    LinkPartitioner partitioner = new LinkPartitioner();
    Assertions.assertThrows(RuntimeException.class, () -> partitioner.configure(configs))
        .getMessage();
  }

  @Test
  void testSingleton() throws InterruptedException {

    var configs =
        Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            "localhost:11111",
            "jmx_servers",
            jmxAddresses);

    var executor = Executors.newFixedThreadPool(10);
    var partitioners = new ArrayList<LinkPartitioner>();
    Partitioner currentSmoothPartitioner = null;

    // create partitions by multi-threads
    IntStream.range(0, 10)
        .forEach(
            i -> {
              executor.execute(
                  () -> {
                    try (MockedConstruction mocked =
                        mockConstruction(LinkPartitioner.ThreadSafeSmoothPartitioner.class)) {
                      var partitioner = new LinkPartitioner();
                      partitioner.configure(configs);
                      synchronized (partitioners) {
                        partitioners.add(partitioner);
                      }
                    }
                  });
            });
    executor.shutdown();
    Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

    for (LinkPartitioner partitioner : partitioners) {
      if (currentSmoothPartitioner == null) {
        currentSmoothPartitioner = partitioner.getPartitioner();
      } else {
        Assertions.assertEquals(currentSmoothPartitioner, partitioner.getPartitioner());
      }
    }

    Partitioner partitioner10 = getFactory().getSmoothPartitionerMap().get(configs);
    verify(partitioner10).configure(any());
    verify(partitioner10, never()).partition(anyString(), any(), any(), any(), any(), any());
    verify(partitioner10, never()).close();

    final Cluster cluster =
        new Cluster(
            "clusterId",
            asList(NODES),
            PARTITIONS,
            Collections.<String>emptySet(),
            Collections.<String>emptySet());

    // try to call partition
    partitioners.get(0).partition("test", null, null, null, null, cluster);
    partitioners.get(1).partition("test", null, null, null, null, cluster);
    partitioners.get(2).partition("test", null, null, null, null, cluster);
    verify(partitioner10).configure(any());
    verify(partitioner10, times(3)).partition(anyString(), any(), any(), any(), any(), any());
    verify(partitioner10, never()).close();

    // ThreadSafePartitioner is not closed if not all PassToProducers are closed.
    var executor2 = Executors.newFixedThreadPool(partitioners.size() - 1);

    partitioners.subList(1, partitioners.size()).forEach(p -> executor2.execute(p::close));

    executor2.shutdown();

    Assertions.assertTrue(executor2.awaitTermination(30, TimeUnit.SECONDS));
    verify(partitioner10).configure(any());
    verify(partitioner10, times(3)).partition(anyString(), any(), any(), any(), any(), any());
    verify(partitioner10, never()).close();

    // ok, all are closed
    partitioners.get(0).close();
    verify(partitioner10).configure(any());
    verify(partitioner10, times(3)).partition(anyString(), any(), any(), any(), any(), any());
    verify(partitioner10).close();
    Assertions.assertNull(getFactory().getSmoothPartitionerMap().get(configs));

    try (MockedConstruction mocked =
        mockConstruction(LinkPartitioner.ThreadSafeSmoothPartitioner.class)) {
      // let us create it again
      var partitioner = new LinkPartitioner();
      partitioner.configure(configs);
      partitioner10 = getFactory().getSmoothPartitionerMap().get(configs);
      verify(partitioner10).configure(any());
      verify(partitioner10, never()).partition(anyString(), any(), any(), any(), any(), any());
      verify(partitioner10, never()).close();

      // close it
      partitioner.close();
      verify(partitioner10).configure(any());
      verify(partitioner10, never()).partition(anyString(), any(), any(), any(), any(), any());
      verify(partitioner10).close();
    }

    try (MockedConstruction mocked =
        mockConstruction(LinkPartitioner.ThreadSafeSmoothPartitioner.class)) {
      // create two ThreadSafePartitioner with two configs

      var partitioner0 = new LinkPartitioner();
      var props0 =
          Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "value0", "jmx_servers", jmxAddresses);
      partitioner0.configure(props0);

      var smoothPartitioner0 = getFactory().getSmoothPartitionerMap().get(props0);

      verify(smoothPartitioner0, times(1)).configure(any());
      verify(smoothPartitioner0, never()).partition(anyString(), any(), any(), any(), any(), any());
      verify(smoothPartitioner0, never()).close();

      var partitioner1 = new LinkPartitioner();
      var props1 =
          Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "value1", "jmx_servers", jmxAddresses);
      partitioner1.configure(props1);
      var smoothPartitioner1 = getFactory().getSmoothPartitionerMap().get(props1);
      Assertions.assertNotEquals(smoothPartitioner0, smoothPartitioner1);

      verify(smoothPartitioner1, times(1)).configure(any());
      verify(smoothPartitioner1, never()).partition(anyString(), any(), any(), any(), any(), any());
      verify(smoothPartitioner1, never()).close();

      // close them
      partitioner0.close();
      verify(smoothPartitioner0).configure(any());
      verify(smoothPartitioner0, never()).partition(anyString(), any(), any(), any(), any(), any());
      verify(smoothPartitioner0).close();
      partitioner1.close();
      verify(smoothPartitioner1).configure(any());
      verify(smoothPartitioner1, never()).partition(anyString(), any(), any(), any(), any(), any());
      verify(smoothPartitioner1).close();
    }
  }
}
