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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.common.DataRate;
import org.astraea.common.DataUnit;
import org.astraea.common.Utils;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.producer.Record;

public interface DataGenerator extends AbstractThread {
  static DataGenerator of(
      BlockingQueue<List<Record<byte[], byte[]>>> queue,
      Supplier<TopicPartition> partitionSelector,
      Performance.Argument argument) {
    var dataSupplier =
        supplier(
            argument.transactionSize,
            argument.keyDistributionType.create(10000),
            argument.keyDistributionType.create(
                argument.keySize.measurement(DataUnit.Byte).intValue()),
            argument.valueDistributionType.create(10000),
            argument.valueDistributionType.create(
                argument.valueSize.measurement(DataUnit.Byte).intValue()),
            argument.throttles,
            argument.throughput);
    var closeLatch = new CountDownLatch(1);
    var executor = Executors.newFixedThreadPool(1);
    var closed = new AtomicBoolean(false);
    var start = System.currentTimeMillis();
    var dataCount = new AtomicLong(0);

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
                      // check the generator is finished or not
                      if (argument.exeTime.percentage(
                              dataCount.getAndIncrement(), System.currentTimeMillis() - start)
                          >= 100D) return;

                      var tp = partitionSelector.get();
                      var records = dataSupplier.apply(tp);

                      // throttled data wouldn't put into the queue
                      if (records.isEmpty()) continue;

                      queue.put(records);
                    }
                  } catch (InterruptedException e) {
                    if (closeLatch.getCount() != 0 || closed.get())
                      throw new RuntimeException(e + ", The data generator didn't close properly");
                  } finally {
                    closeLatch.countDown();
                    closed.set(true);
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

  static Function<TopicPartition, List<Record<byte[], byte[]>>> supplier(
      int batchSize,
      Supplier<Long> keyDistribution,
      Supplier<Long> keySizeDistribution,
      Supplier<Long> valueDistribution,
      Supplier<Long> valueSizeDistribution,
      Map<TopicPartition, DataRate> throughput,
      DataRate defaultThroughput) {
    final Map<TopicPartition, Throttler> throttlers =
        throughput.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new Throttler(e.getValue())));
    final var defaultThrottler = new Throttler(defaultThroughput);
    final Random rand = new Random();
    final Map<Long, byte[]> recordKeyTable = new ConcurrentHashMap<>();
    final Map<Long, byte[]> recordValueTable = new ConcurrentHashMap<>();
    Supplier<byte[]> keySupplier =
        () -> getOrNew(recordKeyTable, keyDistribution, keySizeDistribution.get().intValue(), rand);
    Supplier<byte[]> valueSupplier =
        () ->
            getOrNew(
                recordValueTable, valueDistribution, valueSizeDistribution.get().intValue(), rand);

    return (tp) -> {
      var throttler = throttlers.getOrDefault(tp, defaultThrottler);
      var records = new ArrayList<Record<byte[], byte[]>>(batchSize);

      for (int i = 0; i < batchSize; i++) {
        var key = keySupplier.get();
        var value = valueSupplier.get();
        if (throttler.throttled(
            (value != null ? value.length : 0) + (key != null ? key.length : 0))) return List.of();
        records.add(
            Record.builder()
                .key(key)
                .value(value)
                .topicPartition(tp)
                .timestamp(System.currentTimeMillis())
                .build());
      }
      return records;
    };
  }

  // Find the key from the table, if the record has been produced before. Randomly generate a
  // byte array if
  // the record has not been produced.
  static byte[] getOrNew(
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
}
