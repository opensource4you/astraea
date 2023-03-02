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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.astraea.common.Configuration;
import org.astraea.common.DataRate;
import org.astraea.common.DataUnit;
import org.astraea.common.Utils;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.producer.Record;

public interface DataGenerator extends AbstractThread {
  static DataGenerator of(
      List<ArrayBlockingQueue<List<Record<byte[], byte[]>>>> queues,
      Supplier<TopicPartition> partitionSelector,
      Performance.Argument argument) {
    var keyDistConfig = Configuration.of(argument.keyDistributionConfig);
    var keySizeDistConfig = Configuration.of(argument.keySizeDistributionConfig);
    var valueDistConfig = Configuration.of(argument.valueDistributionConfig);
    var dataSupplier =
        supplier(
            argument.transactionSize,
            argument.recordKeyTableSeed,
            argument.recordValueTableSeed,
            LongStream.rangeClosed(0, 10000).boxed().collect(Collectors.toUnmodifiableList()),
            argument.keyDistributionType.create(10000, keyDistConfig),
            argument.keySizeDistributionType.create(
                (int) argument.keySize.bytes(), keySizeDistConfig),
            LongStream.rangeClosed(0, 10000).boxed().collect(Collectors.toUnmodifiableList()),
            argument.valueDistributionType.create(10000, valueDistConfig),
            argument.valueDistributionType.create(
                argument.valueSize.measurement(DataUnit.Byte).intValue(), valueDistConfig),
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
                      var queue = queues.get(ThreadLocalRandom.current().nextInt(queues.size()));
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
      long keyTableSeed,
      long valueTableSeed,
      List<Long> keyFunctionRange,
      Supplier<Long> keyDistribution,
      Supplier<Long> keySizeDistribution,
      List<Long> valueFunctionRange,
      Supplier<Long> valueDistribution,
      Supplier<Long> valueSizeDistribution,
      Map<TopicPartition, DataRate> throughput,
      DataRate defaultThroughput) {
    final Map<TopicPartition, Throttler> throttlers =
        throughput.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new Throttler(e.getValue())));
    final var defaultThrottler = new Throttler(defaultThroughput);
    final var keyRandom = new Random(keyTableSeed);
    final var valueRandom = new Random(valueTableSeed);
    final Map<Long, byte[]> recordKeyTable =
        keyFunctionRange.stream()
            .map(
                index -> {
                  var size = keySizeDistribution.get().intValue();
                  var key = new byte[size];
                  keyRandom.nextBytes(key);
                  return Map.entry(index, key);
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    final Map<Long, byte[]> recordValueTable =
        valueFunctionRange.stream()
            .map(
                index -> {
                  var size = valueSizeDistribution.get().intValue();
                  var value = new byte[size];
                  valueRandom.nextBytes(value);
                  return Map.entry(index, value);
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    Supplier<byte[]> keySupplier =
        () -> {
          var key = recordKeyTable.get(keyDistribution.get());
          // Intentionally replace key with zero length by null key. A key with zero length can lead
          // to ambiguous behavior in an experiment.
          // See https://github.com/skiptests/astraea/pull/1521#discussion_r1121801293 for further
          // details.
          return key.length > 0 ? key : null;
        };
    Supplier<byte[]> valueSupplier = () -> recordValueTable.get(valueDistribution.get());

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
