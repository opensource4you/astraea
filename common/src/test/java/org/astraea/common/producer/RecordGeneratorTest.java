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
package org.astraea.common.producer;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.astraea.common.DataRate;
import org.astraea.common.DataSize;
import org.astraea.common.DataUnit;
import org.astraea.common.admin.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RecordGeneratorTest {
  @Test
  void testKeySize() {
    var dataSupplier =
        RecordGenerator.builder()
            .batchSize(1)
            .keyRange(List.of(1L))
            .keyDistribution(() -> 1L)
            .keySizeDistribution(() -> DataSize.Byte.of(20).measurement(DataUnit.Byte).longValue())
            .valueRange(List.of(2L))
            .valueDistribution(() -> 2L)
            .valueSizeDistribution(
                () -> DataSize.KiB.of(100).measurement(DataUnit.Byte).longValue())
            .throughput(tp -> DataRate.MB.of(200))
            .build();
    var tp = TopicPartition.of("test-0");
    var data1 = dataSupplier.apply(tp);
    Assertions.assertFalse(data1.isEmpty());
    var data2 = dataSupplier.apply(tp);
    Assertions.assertFalse(data2.isEmpty());
    // key content is fixed, the keys are the same
    Assertions.assertEquals(data1.get(0).key(), data2.get(0).key());
  }

  @Test
  void testFixedValueSize() {
    var dataSupplier =
        RecordGenerator.builder()
            .batchSize(1)
            .keyRange(List.of(1L))
            .keyDistribution(() -> 1L)
            .keySizeDistribution(() -> DataSize.Byte.of(20).measurement(DataUnit.Byte).longValue())
            .valueRange(List.of(2L))
            .valueDistribution(() -> 2L)
            .valueSizeDistribution(
                () -> DataSize.KiB.of(100).measurement(DataUnit.Byte).longValue())
            .throughput(tp -> DataRate.KiB.of(100))
            .build();
    var tp = TopicPartition.of("test-0");
    var data = dataSupplier.apply(tp);
    Assertions.assertFalse(data.isEmpty());
    // initial value size is 100KB and the distributed is fixed to zero, so the final size is 102400
    Assertions.assertEquals(102400, data.get(0).value().length);
  }

  @Test
  void testKeyDistribution() {
    var counter = new AtomicLong(0L);

    // Round-robin on 2 keys. Round-robin key size between 100Byte and 101Byte
    var dataSupplier =
        RecordGenerator.builder()
            .batchSize(1)
            .keyRange(List.of(0L, 1L))
            .keyDistribution(() -> counter.getAndIncrement() % 2)
            .keySizeDistribution(() -> 1 + ThreadLocalRandom.current().nextLong(2))
            .valueRange(List.of(10L))
            .valueDistribution(() -> 10L)
            .valueSizeDistribution(() -> 10L)
            .throughput(tp -> DataRate.KiB.of(200))
            .build();
    var tp = TopicPartition.of("test-0");
    var data1 = dataSupplier.apply(tp);
    Assertions.assertFalse(data1.isEmpty());
    var data2 = dataSupplier.apply(tp);
    Assertions.assertFalse(data2.isEmpty());
    var data3 = dataSupplier.apply(tp);
    Assertions.assertFalse(data3.isEmpty());

    // The key of data1 and data2 should have size 1 byte or 2 byte.
    Assertions.assertTrue(data1.get(0).key().length <= 2);
    Assertions.assertTrue(data2.get(0).key().length <= 2);
    // Round-robin key distribution with 2 possible key.
    Assertions.assertEquals(data1.get(0).key(), data3.get(0).key());

    // Round-robin on 2 keys. Fixed key size to 100 bytes.
    dataSupplier =
        RecordGenerator.builder()
            .batchSize(1)
            .keyRange(List.of(0L, 1L))
            .keyDistribution(() -> counter.getAndIncrement() % 2)
            .keySizeDistribution(() -> 100L)
            .valueRange(List.of(10L))
            .valueDistribution(() -> 10L)
            .valueSizeDistribution(() -> 10L)
            .throughput(ignored -> DataRate.KiB.of(200))
            .build();
    data1 = dataSupplier.apply(tp);
    Assertions.assertFalse(data1.isEmpty());
    data2 = dataSupplier.apply(tp);
    Assertions.assertFalse(data2.isEmpty());
    // Same size but different content
    Assertions.assertEquals(100, data1.get(0).key().length);
    Assertions.assertEquals(100, data2.get(0).key().length);
    Assertions.assertFalse(Arrays.equals(data1.get(0).key(), data2.get(0).key()));
  }

  @Test
  void testDistributedValueSize() {
    var counter = new AtomicLong(0);
    var valueSizeCount = new AtomicLong(0);

    // Round-robin on 2 values. Round-robin value size between 100Byte and 101Byte
    var dataSupplier =
        RecordGenerator.builder()
            .batchSize(1)
            .keyRange(List.of(10L))
            .keyDistribution(() -> 10L)
            .keySizeDistribution(() -> 100L)
            .valueRange(List.of(0L, 1L))
            .valueDistribution(() -> counter.getAndIncrement() % 2)
            .valueSizeDistribution(valueSizeCount::getAndIncrement)
            .throughput(ignored -> DataRate.KiB.of(100))
            .build();
    var tp = TopicPartition.of("test-0");
    var data1 = dataSupplier.apply(tp);
    Assertions.assertFalse(data1.isEmpty());
    var data2 = dataSupplier.apply(tp);
    Assertions.assertFalse(data2.isEmpty());
    var data3 = dataSupplier.apply(tp);
    Assertions.assertFalse(data3.isEmpty());

    // The value of data1 and data2 should have size null or 1 byte.
    Assertions.assertTrue(data1.get(0).value() == null || data1.get(0).value().length == 1);
    Assertions.assertTrue(data2.get(0).value() == null || data2.get(0).value().length == 1);
    // Round-robin value distribution with 2 possible value.
    Assertions.assertEquals(data1.get(0).value(), data3.get(0).value());

    // Round-robin on 2 values. Fixed value size.
    dataSupplier =
        RecordGenerator.builder()
            .batchSize(1)
            .keyRange(List.of(10L))
            .keyDistribution(() -> 10L)
            .keySizeDistribution(() -> 100L)
            .valueRange(List.of(0L, 1L))
            .valueDistribution(() -> counter.getAndIncrement() % 2)
            .valueSizeDistribution(() -> 100L)
            .throughput(ignored -> DataRate.KiB.of(100))
            .build();
    data1 = dataSupplier.apply(tp);
    Assertions.assertFalse(data1.isEmpty());
    data2 = dataSupplier.apply(tp);
    Assertions.assertFalse(data2.isEmpty());
    // Same size but different content
    Assertions.assertEquals(100, data1.get(0).value().length);
    Assertions.assertEquals(100, data2.get(0).value().length);
    Assertions.assertFalse(Arrays.equals(data1.get(0).value(), data2.get(0).value()));
  }

  @Test
  void testThrottle() {
    var throttler = RecordGenerator.throttler(DataRate.KiB.of(150));
    // total: 100KB, limit: 150KB -> no throttle
    Assertions.assertFalse(
        throttler.apply(DataSize.KiB.of(100).measurement(DataUnit.Byte).longValue()));
    // total: 500KB, limit: 150KB -> throttled
    Assertions.assertTrue(
        throttler.apply(DataSize.KiB.of(400).measurement(DataUnit.Byte).longValue()));
  }

  @Test
  void testNoKey() {
    var dataSupplier =
        RecordGenerator.builder()
            .batchSize(1)
            .keyRange(List.of(10L))
            .keyDistribution(() -> 10L)
            .keySizeDistribution(() -> 0L)
            .valueRange(List.of(10L))
            .valueDistribution(() -> 10L)
            .valueSizeDistribution(() -> 10L)
            .throughput(ignored -> DataRate.KiB.of(200))
            .build();

    var tp = TopicPartition.of("test-0");
    var data = dataSupplier.apply(tp);
    Assertions.assertFalse(data.isEmpty());
    Assertions.assertNull(data.get(0).key());
  }

  @Test
  void testNoValue() {
    var dataSupplier =
        RecordGenerator.builder()
            .batchSize(1)
            .keyRange(List.of(10L))
            .keyDistribution(() -> 10L)
            .keySizeDistribution(() -> 0L)
            .valueRange(List.of(10L))
            .valueDistribution(() -> 10L)
            .valueSizeDistribution(() -> 0L)
            .throughput(ignored -> DataRate.KiB.of(200))
            .build();
    var tp = TopicPartition.of("test-0");
    var data = dataSupplier.apply(tp);
    Assertions.assertFalse(data.isEmpty());
    Assertions.assertNull(data.get(0).value());
  }

  @Test
  void testBatch() {
    var dataSupplier =
        RecordGenerator.builder()
            .batchSize(3)
            .keyRange(List.of(1L))
            .keyDistribution(() -> 1L)
            .keySizeDistribution(() -> 1L)
            .valueRange(List.of(1L))
            .valueDistribution(() -> 1L)
            .valueSizeDistribution(() -> 1L)
            .throughput(ignored -> DataRate.KiB.of(100))
            .build();
    var tp = TopicPartition.of("test-0");
    var data = dataSupplier.apply(tp);
    Assertions.assertEquals(3, data.size());
  }

  @Test
  void testRandomSeed() {
    long keyContentSeed = ThreadLocalRandom.current().nextLong();
    long valueContentSeed = ThreadLocalRandom.current().nextLong();
    int size = 10000;
    var keyRandom0 = new Random(keyContentSeed);
    var valueRandom0 = new Random(valueContentSeed);
    var keyRandom1 = new Random(keyContentSeed);
    var valueRandom1 = new Random(valueContentSeed);
    var gen0 =
        RecordGenerator.builder()
            .batchSize(10)
            .keyTableSeed(keyContentSeed)
            .keyRange(LongStream.rangeClosed(0, size).boxed().toList())
            .keyDistribution(() -> Math.abs(keyRandom0.nextLong() % size))
            .keySizeDistribution(() -> 32L)
            .valueTableSeed(valueContentSeed)
            .valueRange(LongStream.rangeClosed(0, size).boxed().toList())
            .valueDistribution(() -> Math.abs(valueRandom0.nextLong() % size))
            .valueSizeDistribution(() -> 256L)
            .build();
    var gen1 =
        RecordGenerator.builder()
            .batchSize(10)
            .keyTableSeed(keyContentSeed)
            .keyRange(LongStream.rangeClosed(0, size).boxed().toList())
            .keyDistribution(() -> Math.abs(keyRandom1.nextLong() % size))
            .keySizeDistribution(() -> 32L)
            .valueTableSeed(valueContentSeed)
            .valueRange(LongStream.rangeClosed(0, size).boxed().toList())
            .valueDistribution(() -> Math.abs(valueRandom1.nextLong() % size))
            .valueSizeDistribution(() -> 256L)
            .build();

    var tp = TopicPartition.of("A", -1);
    var series0 = IntStream.range(0, 10000).mapToObj(i -> gen0.apply(tp)).toList();
    var series1 = IntStream.range(0, 10000).mapToObj(i -> gen1.apply(tp)).toList();
    for (int i = 0; i < 10000; i++) {
      for (int j = 0; j < 10; j++) {
        Assertions.assertArrayEquals(series0.get(i).get(j).key(), series1.get(i).get(j).key());
        Assertions.assertArrayEquals(series0.get(i).get(j).value(), series1.get(i).get(j).value());
      }
    }
  }

  @Test
  void testAllDefault() {
    var generator = RecordGenerator.builder().build();
    Assertions.assertNotEquals(0, generator.apply(TopicPartition.of("a", 0)).size());
  }

  @Test
  void testEmptyKeyAndValue() {
    var generator =
        RecordGenerator.builder()
            .keySizeDistribution(() -> 0L)
            .valueSizeDistribution(() -> 0L)
            .build();
    var records = generator.apply(TopicPartition.of("t", 0));
    Assertions.assertNotEquals(0, records.size());
    records.forEach(
        r -> {
          Assertions.assertNull(r.key());
          Assertions.assertNull(r.value());
        });
  }
}
