package org.astraea.partitioner;

import java.util.ArrayList;
import java.util.Comparator;
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
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PartitionerFactoryTest {
  private static long countOfPartition = 0;
  private static long countOfOnConfigure = 0;
  private static long countOfClose = 0;

  @BeforeEach
  void reset() {
    countOfPartition = 0;
    countOfOnConfigure = 0;
    countOfClose = 0;
  }

  @Test
  void testSingletonByProducer() {
    var props =
        Map.<String, Object>of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            "localhost:11111",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            ByteArraySerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            ByteArraySerializer.class.getName(),
            ProducerConfig.PARTITIONER_CLASS_CONFIG,
            PassToProducer.class.getName());

    // create multiples partitioners
    var producers =
        IntStream.range(0, 10)
            .mapToObj(i -> new KafkaProducer<byte[], byte[]>(props))
            .collect(Collectors.toList());

    // ThreadSafePartitioner is created only once
    Assertions.assertEquals(1, countOfOnConfigure);
    Assertions.assertEquals(0, countOfPartition);
    Assertions.assertEquals(0, countOfClose);

    producers.forEach(Producer::close);
    // ThreadSafePartitioner is closed only once
    Assertions.assertEquals(1, countOfOnConfigure);
    Assertions.assertEquals(0, countOfPartition);
    Assertions.assertEquals(1, countOfClose);
  }

  @Test
  void testSingleton() throws InterruptedException {
    var configs = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "b");

    var executor = Executors.newFixedThreadPool(10);
    var partitioners = new ArrayList<Partitioner>();

    // create partitions by multi-threads
    IntStream.range(0, 10)
        .forEach(
            i -> {
              executor.execute(
                  () -> {
                    var partitioner = new PassToProducer();
                    partitioner.configure(configs);
                    synchronized (partitioners) {
                      partitioners.add(partitioner);
                    }
                  });
            });
    executor.shutdown();
    Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

    // ThreadSafePartitioner is created only once
    Assertions.assertEquals(1, countOfOnConfigure);
    Assertions.assertEquals(0, countOfPartition);
    Assertions.assertEquals(0, countOfClose);

    // try to call partition
    partitioners.get(0).partition(null, null, null, null, null, null);
    partitioners.get(1).partition(null, null, null, null, null, null);
    partitioners.get(2).partition(null, null, null, null, null, null);
    Assertions.assertEquals(1, countOfOnConfigure);
    Assertions.assertEquals(3, countOfPartition);
    Assertions.assertEquals(0, countOfClose);

    // ThreadSafePartitioner is not closed if not all PassToProducers are closed.
    partitioners.subList(1, partitioners.size()).forEach(Partitioner::close);
    Assertions.assertEquals(1, countOfOnConfigure);
    Assertions.assertEquals(3, countOfPartition);
    Assertions.assertEquals(0, countOfClose);

    // ok, all are closed
    partitioners.get(0).close();
    Assertions.assertEquals(1, countOfOnConfigure);
    Assertions.assertEquals(3, countOfPartition);
    Assertions.assertEquals(1, countOfClose);

    // let us create it again
    var partitioner = new PassToProducer();
    partitioner.configure(configs);
    Assertions.assertEquals(2, countOfOnConfigure);
    Assertions.assertEquals(3, countOfPartition);
    Assertions.assertEquals(1, countOfClose);

    // close it
    partitioner.close();
    Assertions.assertEquals(2, countOfOnConfigure);
    Assertions.assertEquals(3, countOfPartition);
    Assertions.assertEquals(2, countOfClose);

    // create two ThreadSafePartitioner with two configs
    var partitioner0 = new PassToProducer();
    partitioner0.configure(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "value0"));
    var partitioner1 = new PassToProducer();
    partitioner1.configure(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "value1"));
    Assertions.assertEquals(4, countOfOnConfigure);
    Assertions.assertEquals(3, countOfPartition);
    Assertions.assertEquals(2, countOfClose);

    // close them
    partitioner0.close();
    partitioner1.close();
    Assertions.assertEquals(4, countOfOnConfigure);
    Assertions.assertEquals(3, countOfPartition);
    Assertions.assertEquals(4, countOfClose);
  }

  public static class PassToProducer implements Partitioner {

    private static final PartitionerFactory FACTORY =
        new PartitionerFactory(
            Comparator.comparing(o -> o.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).toString()));

    private Partitioner partitioner;

    @Override
    public int partition(
        String topic,
        Object key,
        byte[] keyBytes,
        Object value,
        byte[] valueBytes,
        Cluster cluster) {
      return partitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
    }

    @Override
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
      partitioner.onNewBatch(topic, cluster, prevPartition);
    }

    @Override
    public void close() {
      partitioner.close();
    }

    @Override
    public void configure(Map<String, ?> configs) {
      partitioner = FACTORY.getOrCreate(ThreadSafePartitioner.class, configs);
    }
  }

  static class ThreadSafePartitioner implements Partitioner {

    @Override
    public int partition(
        String topic,
        Object key,
        byte[] keyBytes,
        Object value,
        byte[] valueBytes,
        Cluster cluster) {
      countOfPartition += 1;
      return 0;
    }

    @Override
    public void close() {
      countOfClose += 1;
    }

    @Override
    public void configure(Map<String, ?> configs) {
      countOfOnConfigure += 1;
    }
  }
}
