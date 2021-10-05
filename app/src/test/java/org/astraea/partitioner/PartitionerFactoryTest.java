package org.astraea.partitioner;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionerFactoryTest {
  private static long countOfPartition = 0;
  private static long countOfOnConfigure = 0;
  private static long countOfClose = 0;

  @Test
  void test() {
    var configs = Map.of("a", "b");

    // create multiples partitioners
    var partitioners =
        IntStream.range(0, 10)
            .mapToObj(
                i -> {
                  var partitioner = new PassToProducer();
                  partitioner.configure(configs);
                  return partitioner;
                })
            .collect(Collectors.toList());

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
    partitioner0.configure(Map.of("key", "value0"));
    var partitioner1 = new PassToProducer();
    partitioner1.configure(Map.of("key", "value1"));
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

  private static class PassToProducer implements Partitioner {

    private static final PartitionerFactory FACTORY = new PartitionerFactory();

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
