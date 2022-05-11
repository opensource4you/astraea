package org.astraea.performance;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.kafka.common.TopicPartition;
import org.astraea.Utils;
import org.astraea.concurrent.State;
import org.astraea.producer.Producer;
import org.astraea.service.RequireBrokerCluster;
import org.astraea.topic.TopicAdmin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ProducerExecutorTest extends RequireBrokerCluster {

  @ParameterizedTest
  @MethodSource("offerProducerExecutors")
  void testSpecifiedPartition(ProducerExecutor executor) throws InterruptedException {
    var specifiedPartition = 1;
    ((MyPartitionSupplier) executor.partitionSupplier()).partition = specifiedPartition;
    try (var admin = TopicAdmin.of(bootstrapServers())) {
      admin.creator().topic(executor.topic()).numberOfPartitions(specifiedPartition + 1).create();
      // wait for topic creation
      TimeUnit.SECONDS.sleep(2);
      admin
          .offsets(Set.of(executor.topic()))
          .values()
          .forEach(o -> Assertions.assertEquals(0, o.latest()));
      Assertions.assertEquals(State.RUNNING, executor.execute());
      // wait for syncing data
      TimeUnit.SECONDS.sleep(2);
      // only specified partition gets value
      // for normal producer, there is only one record
      // for transactional producer, the size of transaction is 10 and there is one transaction
      // control marker
      Assertions.assertEquals(
          executor.transactional() ? 11 : 1,
          admin
              .offsets(Set.of(executor.topic()))
              .get(new TopicPartition(executor.topic(), specifiedPartition))
              .latest());
      // other partitions have no data
      admin.offsets(Set.of(executor.topic())).entrySet().stream()
          .filter(e -> !e.getKey().equals(new TopicPartition(executor.topic(), specifiedPartition)))
          .forEach(e -> Assertions.assertEquals(0, e.getValue().latest()));
    }
  }

  @ParameterizedTest
  @MethodSource("offerProducerExecutors")
  void testDone(ProducerExecutor executor) throws InterruptedException {
    ((MyDataSupplier) executor.dataSupplier()).data = DataSupplier.NO_MORE_DATA;
    Assertions.assertEquals(State.DONE, executor.execute());
  }

  @ParameterizedTest
  @MethodSource("offerProducerExecutors")
  void testClose(ProducerExecutor executor) {
    executor.close();
    Assertions.assertTrue(executor.closed());
  }

  @ParameterizedTest
  @MethodSource("offerProducerExecutors")
  void testObserver(ProducerExecutor executor) throws InterruptedException {
    Assertions.assertEquals(State.RUNNING, executor.execute());
    // wait for async call
    TimeUnit.SECONDS.sleep(2);
    Assertions.assertEquals(
        executor.transactional() ? 10 : 1, ((Observer) executor.observer()).recordsHook.size());
    Assertions.assertEquals(
        executor.transactional() ? 10 : 1, ((Observer) executor.observer()).elapsedHook.size());
  }

  private static class Observer implements BiConsumer<Long, Long> {
    private final BlockingQueue<Long> recordsHook = new LinkedBlockingDeque<>();
    private final BlockingQueue<Long> elapsedHook = new LinkedBlockingDeque<>();

    @Override
    public void accept(Long records, Long elapsed) {
      Assertions.assertTrue(recordsHook.offer(records));
      Assertions.assertTrue(elapsedHook.offer(elapsed));
    }
  }

  private static class MyDataSupplier implements DataSupplier {

    private Data data =
        DataSupplier.data(
            "key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8));

    @Override
    public Data get() {
      return data;
    }
  }

  private static class MyPartitionSupplier implements Supplier<Integer> {
    private int partition = -1;

    @Override
    public Integer get() {
      return partition;
    }
  }

  private static Stream<Arguments> offerProducerExecutors() {
    var normalTopic = Utils.randomString(10);
    var transactionalTopic = Utils.randomString(10);
    return Stream.of(
        Arguments.of(
            Named.of(
                "normal producer for topic: " + normalTopic,
                ProducerExecutor.of(
                    normalTopic,
                    1,
                    Producer.builder().bootstrapServers(bootstrapServers()).build(),
                    new Observer(),
                    new MyPartitionSupplier(),
                    new MyDataSupplier()))),
        Arguments.of(
            Named.of(
                "transactional producer for topic: " + transactionalTopic,
                ProducerExecutor.of(
                    transactionalTopic,
                    10,
                    Producer.builder().bootstrapServers(bootstrapServers()).buildTransactional(),
                    new Observer(),
                    new MyPartitionSupplier(),
                    new MyDataSupplier()))));
  }
}
