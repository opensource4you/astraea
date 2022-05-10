package org.astraea.performance;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.Utils;
import org.astraea.concurrent.Executor;
import org.astraea.concurrent.State;
import org.astraea.producer.Producer;
import org.astraea.producer.Sender;

abstract class ProducerExecutor implements Executor {

  static ProducerExecutor of(
      String topic,
      int transactionSize,
      Producer<byte[], byte[]> producer,
      BiConsumer<Long, Long> observer,
      Supplier<Integer> partitionSupplier,
      DataSupplier dataSupplier) {
    return new ProducerExecutor(topic, producer, partitionSupplier, observer, dataSupplier) {

      @Override
      public State execute() {
        var data =
            IntStream.range(0, transactionSize)
                .mapToObj(i -> dataSupplier.get())
                .collect(Collectors.toUnmodifiableList());

        // no more data
        if (data.stream().allMatch(DataSupplier.Data::done)) return State.DONE;

        // no data due to throttle
        // TODO: we should return a precise sleep time
        if (data.stream().allMatch(DataSupplier.Data::throttled)) {
          Utils.sleep(Duration.ofSeconds(1));
          return State.RUNNING;
        }
        return doSend(
            senders(
                data.stream()
                    .filter(DataSupplier.Data::hasData)
                    .collect(Collectors.toUnmodifiableList())));
      }

      List<Sender<byte[], byte[]>> senders(List<DataSupplier.Data> data) {
        return data.stream()
            .map(
                d ->
                    producer
                        .sender()
                        .topic(topic)
                        .partition(partitionSupplier.get())
                        .key(d.key())
                        .value(d.value())
                        .timestamp(System.currentTimeMillis()))
            .collect(Collectors.toList());
      }

      State doSend(List<Sender<byte[], byte[]>> senders) {
        producer
            .send(senders)
            .forEach(
                future ->
                    future.whenComplete(
                        (m, e) ->
                            observer.accept(
                                System.currentTimeMillis() - m.timestamp(),
                                m.serializedValueSize())));
        return State.RUNNING;
      }
    };
  }

  static ProducerExecutor of(
      String topic,
      Producer<byte[], byte[]> producer,
      BiConsumer<Long, Long> observer,
      Supplier<Integer> partitionSupplier,
      DataSupplier dataSupplier) {
    return new ProducerExecutor(topic, producer, partitionSupplier, observer, dataSupplier) {

      @Override
      public State execute() {
        var data = dataSupplier.get();
        if (data.done()) return State.DONE;

        // no data due to throttle
        // TODO: we should return a precise sleep time
        if (data.throttled()) {
          Utils.sleep(Duration.ofSeconds(1));
          return State.RUNNING;
        }
        return doSend(data.key(), data.value());
      }

      Sender<byte[], byte[]> sender(byte[] key, byte[] value) {
        return producer
            .sender()
            .topic(topic)
            .partition(partitionSupplier.get())
            .key(key)
            .value(value)
            .timestamp(System.currentTimeMillis());
      }

      State doSend(byte[] key, byte[] value) {
        sender(key, value)
            .run()
            .whenComplete(
                (m, e) ->
                    observer.accept(
                        System.currentTimeMillis() - m.timestamp(), m.serializedValueSize()));
        return State.RUNNING;
      }
    };
  }

  private final String topic;
  private final Producer<byte[], byte[]> producer;
  private final Supplier<Integer> partitionSupplier;
  private final BiConsumer<Long, Long> observer;
  private final DataSupplier dataSupplier;

  ProducerExecutor(
      String topic,
      Producer<byte[], byte[]> producer,
      Supplier<Integer> partitionSupplier,
      BiConsumer<Long, Long> observer,
      DataSupplier dataSupplier) {
    this.topic = topic;
    this.producer = producer;
    this.partitionSupplier = partitionSupplier;
    this.observer = observer;
    this.dataSupplier = dataSupplier;
  }

  private final AtomicBoolean closed = new AtomicBoolean(false);

  String topic() {
    return topic;
  }

  Supplier<Integer> partitionSupplier() {
    return partitionSupplier;
  }

  BiConsumer<Long, Long> observer() {
    return observer;
  }

  DataSupplier dataSupplier() {
    return dataSupplier;
  }

  /** @return true if the producer in this executor is transactional. */
  boolean transactional() {
    return producer.transactional();
  }

  /** @return true if this executor is closed. otherwise, false */
  public boolean closed() {
    return closed.get();
  }

  @Override
  public void close() {
    try {
      producer.close();
    } finally {
      closed.set(true);
    }
  }
}
