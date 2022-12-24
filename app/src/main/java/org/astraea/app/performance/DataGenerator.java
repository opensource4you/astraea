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
package org.astraea.app.performance;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.DataRate;
import org.astraea.common.DataUnit;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ReplicaInfo;
import org.astraea.common.admin.TopicPartition;

public interface DataGenerator extends AbstractThread {
  // TODO: be care for queue size?
  static DataGenerator of(
      BlockingQueue<List<DataSupplier.Data>> queue, Performance.Argument argument) {
    // TODO: need to detect the number of producers?
    // TODO: like ProducerThread
    var dataSupplier =
        DataSupplier.of(
            argument.transactionSize,
            argument.exeTime,
            argument.keyDistributionType.create(10000),
            argument.keyDistributionType.create(
                argument.keySize.measurement(DataUnit.Byte).intValue()),
            argument.valueDistributionType.create(10000),
            argument.valueDistributionType.create(
                argument.valueSize.measurement(DataUnit.Byte).intValue()),
            argument.throttles,
            argument.throughput);
    var partitionSupplier = PartitionSupplier.of(argument);
    // TODO: the number of thread could be configured?
    var closeLatch = new CountDownLatch(1);
    var executor = Executors.newFixedThreadPool(1);
    var closed = new AtomicBoolean(false);
    // monitor the data generator if close or not
    CompletableFuture.runAsync(
        () -> {
          try {
            Utils.swallowException(closeLatch::await);
          } finally {
            executor.shutdown();
            Utils.swallowException(() -> executor.awaitTermination(30, TimeUnit.SECONDS));
          }
        });
    // put the data into blocking queue
    CompletableFuture.runAsync(
        () ->
            executor.execute(
                () -> {
                  try {
                    while (!closed.get()) {
                      var tp = partitionSupplier.get();
                      var data = dataSupplier.apply(tp);
                      // throttled data wouldn't put into the queue
                      if (data.stream().allMatch(DataSupplier.Data::throttled)) continue;

                      try {
                        if (queue.offer(data, 5, TimeUnit.SECONDS)
                            && data.stream().allMatch(DataSupplier.Data::done)) return;
                      } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                      }
                    }
                  } finally {
                    closeLatch.countDown();
                    closed.set(true);
                    System.out.println("close data generator");
                  }
                }));
    return new DataGenerator() {
      @Override
      public void waitForDone() {
        Utils.swallowException(closeLatch::await);
      }

      @Override
      public boolean closed() {
        return closeLatch.getCount() == 0;
      }

      @Override
      public void close() {
        closed.set(true);
        waitForDone();
      }
    };
  }

  @FunctionalInterface
  interface DataSupplier extends Function<TopicPartition, List<DataSupplier.Data>> {
    static Data data(byte[] key, byte[] value, TopicPartition tp) {
      return new Data() {
        @Override
        public TopicPartition topicPartition() {
          return tp;
        }

        @Override
        public boolean done() {
          return false;
        }

        @Override
        public boolean throttled() {
          return false;
        }

        @Override
        public byte[] key() {
          return key;
        }

        @Override
        public byte[] value() {
          return value;
        }
      };
    }

    Data NO_MORE_DATA =
        new Data() {
          @Override
          public TopicPartition topicPartition() {
            throw new IllegalStateException("there is no data");
          }

          @Override
          public boolean done() {
            return true;
          }

          @Override
          public boolean throttled() {
            return false;
          }

          @Override
          public byte[] key() {
            throw new IllegalStateException("there is no data");
          }

          @Override
          public byte[] value() {
            throw new IllegalStateException("there is no data");
          }
        };

    Data THROTTLED_DATA =
        new Data() {
          @Override
          public TopicPartition topicPartition() {
            throw new IllegalStateException("it is throttled");
          }

          @Override
          public boolean done() {
            return false;
          }

          @Override
          public boolean throttled() {
            return true;
          }

          @Override
          public byte[] key() {
            throw new IllegalStateException("it is throttled");
          }

          @Override
          public byte[] value() {
            throw new IllegalStateException("it is throttled");
          }
        };

    interface Data {
      TopicPartition topicPartition();
      /**
       * @return true if there is no data.
       */
      boolean done();

      /**
       * @return true if there are some data, but it is throttled now.
       */
      boolean throttled();

      /**
       * @return true if there is accessible data
       */
      default boolean hasData() {
        return !done() && !throttled();
      }

      /**
       * @return key or throw exception if there is no data, or it is throttled now
       */
      byte[] key();

      /**
       * @return value or throw exception if there is no data, or it is throttled now
       */
      byte[] value();
    }

    /**
     * Generate Data according to the given arguments. The returned supplier map the 64-bit number
     * supplied by key(/value) distribution to a byte array. That is, if we want the DataSupplier to
     * produce the same content, the key(/value) distribution should always produce the same number.
     * For example,
     *
     * <pre>{@code
     * DataSupplier.of(batchSize,
     *                 exeTime,
     *                 ()->1,
     *                 keySizeDistribution,
     *                 ()->1,
     *                 valueSizeDistribution,
     *                 throughput,
     *                 defaultThroughput)
     *
     * }</pre>
     *
     * It is not recommend to supply too many unique number. This DataSupplier store every unique
     * number and its content in a map structure.
     *
     * @param batchSize
     * @param exeTime the time for stop supplying data
     * @param keyDistribution supply abstract keys which is represented by a 64-bit integer
     * @param keySizeDistribution supply the size of newly created key
     * @param valueDistribution supply abstract value which is represented by a 64-bit integer
     * @param valueSizeDistribution supply the size of newly created value
     * @param throughput the limit throughput of specify topic-partition
     * @param defaultThroughput the default limit on data produced
     * @return supply data with given distribution. It will map the 64-bit number supplied by
     *     key(/value) distribution to a list of byte array.
     */
    static DataSupplier of(
        int batchSize,
        ExeTime exeTime,
        Supplier<Long> keyDistribution,
        Supplier<Long> keySizeDistribution,
        Supplier<Long> valueDistribution,
        Supplier<Long> valueSizeDistribution,
        Map<TopicPartition, DataRate> throughput,
        DataRate defaultThroughput) {
      return new DataSupplier() {
        private final Map<TopicPartition, Throttler> throttlers =
            throughput.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new Throttler(e.getValue())));
        private final Throttler defaultThrottler = new Throttler(defaultThroughput);
        private final long start = System.currentTimeMillis();
        private final Random rand = new Random();
        private final AtomicLong dataCount = new AtomicLong(0);
        private final Map<Long, byte[]> recordKeyTable = new ConcurrentHashMap<>();
        private final Map<Long, byte[]> recordValueTable = new ConcurrentHashMap<>();

        byte[] value() {
          return getOrNew(
              recordValueTable, valueDistribution, valueSizeDistribution.get().intValue(), rand);
        }

        public byte[] key() {
          return getOrNew(
              recordKeyTable, keyDistribution, keySizeDistribution.get().intValue(), rand);
        }

        @Override
        public List<Data> apply(TopicPartition topicPartition) {
          if (exeTime.percentage(dataCount.getAndIncrement(), System.currentTimeMillis() - start)
              >= 100D) return List.of(NO_MORE_DATA);
          return IntStream.range(0, batchSize)
              .mapToObj(
                  i -> {
                    var key = key();
                    var value = value();
                    var throttler = throttlers.getOrDefault(topicPartition, defaultThrottler);

                    if (throttler.throttled(
                        (value != null ? value.length : 0) + (key != null ? key.length : 0)))
                      return THROTTLED_DATA;
                    return data(key, value, topicPartition);
                  })
              .collect(Collectors.toUnmodifiableList());
        }
      };
    }
  }
  // Find the key from the table, if the record has been produced before. Randomly generate a
  // byte array if
  // the record has not been produced.
  private static byte[] getOrNew(
      Map<Long, byte[]> table, Supplier<Long> distribution, int size, Random rand) {
    return table.computeIfAbsent(
        distribution.get(),
        ignore -> {
          if (size == 0) return null;
          var value = new byte[size];
          rand.nextBytes(value);
          return value;
        });
  }

  class Throttler {
    private final long start = System.currentTimeMillis();
    private final long throughput;
    private final AtomicLong totalBytes = new AtomicLong();

    Throttler(DataRate max) {
      throughput = Double.valueOf(max.byteRate()).longValue();
    }

    /**
     * @param payloadLength of new data
     * @return true if the data need to be throttled. Otherwise, false
     */
    boolean throttled(long payloadLength) {
      var duration = durationInSeconds();
      if (duration <= 0) return false;
      var current = totalBytes.addAndGet(payloadLength);
      // too much -> slow down
      if ((current / duration) > throughput) {
        totalBytes.addAndGet(-payloadLength);
        return true;
      }
      return false;
    }

    // visible for testing
    long durationInSeconds() {
      return (System.currentTimeMillis() - start) / 1000;
    }
  }

  @FunctionalInterface
  interface PartitionSupplier extends Supplier<TopicPartition> {
    /**
     * @return a supplier that randomly return a sending target
     */
    static PartitionSupplier of(Performance.Argument argument) {
      var specifiedByBroker = !argument.specifyBrokers.isEmpty();
      var specifiedByPartition = !argument.specifyPartitions.isEmpty();
      if (specifiedByBroker && specifiedByPartition)
        throw new IllegalArgumentException(
            "`--specify.partitions` can't be used in conjunction with `--specify.brokers`");
      else if (specifiedByBroker) {
        try (var admin = Admin.of(argument.configs())) {
          final var selections =
              admin
                  .clusterInfo(Set.copyOf(argument.topics))
                  .toCompletableFuture()
                  .join()
                  .replicaStream()
                  .filter(ReplicaInfo::isLeader)
                  .filter(replica -> argument.specifyBrokers.contains(replica.nodeInfo().id()))
                  .map(replica -> TopicPartition.of(replica.topic(), replica.partition()))
                  .distinct()
                  .collect(Collectors.toUnmodifiableList());
          if (selections.isEmpty())
            throw new IllegalArgumentException(
                "No partition match the specify.brokers requirement");

          return () -> selections.get(ThreadLocalRandom.current().nextInt(selections.size()));
        }
      } else if (specifiedByPartition) {
        // specify.partitions can't be use in conjunction with partitioner or topics
        if (argument.partitioner != null)
          throw new IllegalArgumentException(
              "--specify.partitions can't be used in conjunction with partitioner");
        // sanity check, ensure all specified partitions are existed
        try (var admin = Admin.of(argument.configs())) {
          var allTopics = admin.topicNames(false).toCompletableFuture().join();
          var allTopicPartitions =
              admin
                  .clusterInfo(
                      argument.specifyPartitions.stream()
                          .map(TopicPartition::topic)
                          .filter(allTopics::contains)
                          .collect(Collectors.toUnmodifiableSet()))
                  .toCompletableFuture()
                  .join()
                  .replicaStream()
                  .map(replica -> TopicPartition.of(replica.topic(), replica.partition()))
                  .collect(Collectors.toSet());
          var notExist =
              argument.specifyPartitions.stream()
                  .filter(tp -> !allTopicPartitions.contains(tp))
                  .collect(Collectors.toUnmodifiableSet());
          if (!notExist.isEmpty())
            throw new IllegalArgumentException(
                "The following topic/partitions are nonexistent in the cluster: " + notExist);
        }

        final var selection =
            argument.specifyPartitions.stream().distinct().collect(Collectors.toUnmodifiableList());
        return () -> selection.get(ThreadLocalRandom.current().nextInt(selection.size()));
      } else {
        try (var admin = Admin.of(argument.configs())) {
          final var selection =
              admin
                  .clusterInfo(Set.copyOf(argument.topics))
                  .toCompletableFuture()
                  .join()
                  .replicaStream()
                  .map(ReplicaInfo::topicPartition)
                  .distinct()
                  .collect(Collectors.toUnmodifiableList());
          return () -> selection.get(ThreadLocalRandom.current().nextInt(selection.size()));
        }
      }
    }
  }
}
