package org.astraea.partitioner.nodeLoadMetric;

import static java.util.Arrays.asList;
import static org.astraea.partitioner.partitionerFactory.LinkPartitioner.getCountOfClose;
import static org.astraea.partitioner.partitionerFactory.LinkPartitioner.getCountOfOnConfigure;
import static org.astraea.partitioner.partitionerFactory.LinkPartitioner.getCountOfPartition;
import static org.astraea.partitioner.partitionerFactory.LinkPartitioner.setCountOfClose;
import static org.astraea.partitioner.partitionerFactory.LinkPartitioner.setCountOfOnConfigure;
import static org.astraea.partitioner.partitionerFactory.LinkPartitioner.setCountOfPartition;
import static org.mockito.Mockito.mockConstruction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
  private HashMap<String, String> jmxAddresses;

  private static final byte[] KEY_BYTES = "key".getBytes();
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
    setCountOfPartition(0);
    setCountOfOnConfigure(0);
    setCountOfClose(0);
    jmxAddresses = new HashMap<>();
    jmxAddresses.put("0", "0.0.0.0");
    jmxAddresses.put("1", "0.0.0.0");
    jmxAddresses.put("2", "0.0.0.0");
  }

  @Test
  void testSingletonByProducer() {
    try (MockedConstruction mocked = mockConstruction(NodeMetrics.class)) {
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
              "jmx_server",
              jmxAddresses);

      // create multiples partitioners
      var producers =
          IntStream.range(0, 10)
              .mapToObj(i -> new KafkaProducer<byte[], byte[]>(props))
              .collect(Collectors.toList());

      // ThreadSafePartitioner is created only once
      Assertions.assertEquals(1, getCountOfOnConfigure());
      Assertions.assertEquals(0, getCountOfPartition());
      Assertions.assertEquals(0, getCountOfClose());

      producers.forEach(Producer::close);
      // ThreadSafePartitioner is closed only once
      Assertions.assertEquals(1, getCountOfOnConfigure());
      Assertions.assertEquals(0, getCountOfPartition());
      Assertions.assertEquals(1, getCountOfClose());
    }
  }

  @Test
  void testSingleton() throws InterruptedException {

    var configs =
        Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:11111", "jmx_server", jmxAddresses);

    var executor = Executors.newFixedThreadPool(10);
    var partitioners = new ArrayList<Partitioner>();
    // create partitions by multi-threads
    IntStream.range(0, 10)
        .forEach(
            i -> {
              executor.execute(
                  () -> {
                    try (MockedConstruction mocked = mockConstruction(NodeMetrics.class)) {
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

    // ThreadSafePartitioner is created only once
    Assertions.assertEquals(1, getCountOfOnConfigure());
    Assertions.assertEquals(0, getCountOfPartition());
    Assertions.assertEquals(0, getCountOfClose());

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
    Assertions.assertEquals(1, getCountOfOnConfigure());
    Assertions.assertEquals(3, getCountOfPartition());
    Assertions.assertEquals(0, getCountOfClose());

    // ThreadSafePartitioner is not closed if not all PassToProducers are closed.
    var executor2 = Executors.newFixedThreadPool(partitioners.size() - 1);

    partitioners.subList(1, partitioners.size()).forEach(p -> executor2.execute(p::close));

    executor2.shutdown();

    Assertions.assertTrue(executor2.awaitTermination(30, TimeUnit.SECONDS));
    Assertions.assertEquals(1, getCountOfOnConfigure());
    Assertions.assertEquals(3, getCountOfPartition());
    Assertions.assertEquals(0, getCountOfClose());

    // ok, all are closed
    partitioners.get(0).close();
    Assertions.assertEquals(1, getCountOfOnConfigure());
    Assertions.assertEquals(3, getCountOfPartition());
    Assertions.assertEquals(1, getCountOfClose());

    try (MockedConstruction mocked = mockConstruction(NodeMetrics.class)) {
      // let us create it again
      var partitioner = new LinkPartitioner();
      partitioner.configure(configs);
      Assertions.assertEquals(2, getCountOfOnConfigure());
      Assertions.assertEquals(3, getCountOfPartition());
      Assertions.assertEquals(1, getCountOfClose());

      // close it
      partitioner.close();
      Assertions.assertEquals(2, getCountOfOnConfigure());
      Assertions.assertEquals(3, getCountOfPartition());
      Assertions.assertEquals(2, getCountOfClose());
    }

    try (MockedConstruction mocked = mockConstruction(NodeMetrics.class)) {
      // create two ThreadSafePartitioner with two configs

      var partitioner0 = new LinkPartitioner();
      partitioner0.configure(
          Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "value0", "jmx_server", jmxAddresses));
      var partitioner1 = new LinkPartitioner();
      partitioner1.configure(
          Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "value1", "jmx_server", jmxAddresses));
      Assertions.assertEquals(4, getCountOfOnConfigure());
      Assertions.assertEquals(3, getCountOfPartition());
      Assertions.assertEquals(2, getCountOfClose());

      // close them
      partitioner0.close();
      partitioner1.close();
      Assertions.assertEquals(4, getCountOfOnConfigure());
      Assertions.assertEquals(3, getCountOfPartition());
      Assertions.assertEquals(4, getCountOfClose());
    }
  }
}
