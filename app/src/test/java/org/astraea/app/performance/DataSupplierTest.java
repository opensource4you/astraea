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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.astraea.common.DataRate;
import org.astraea.common.DataSize;
import org.astraea.common.DataUnit;
import org.astraea.common.admin.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DataSupplierTest {
  @Test
  void testKeySize() {
    var dataSupplier =
        DataGenerator.supplier(
            1,
            0,
            0,
            List.of(1L),
            () -> 1L,
            () -> DataSize.Byte.of(20).measurement(DataUnit.Byte).longValue(),
            List.of(2L),
            () -> 2L,
            () -> DataSize.KiB.of(100).measurement(DataUnit.Byte).longValue(),
            Map.of(),
            DataRate.KiB.of(200).perSecond());
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
        DataGenerator.supplier(
            1,
            0,
            0,
            List.of(1L),
            () -> 1L,
            () -> DataSize.Byte.of(20).measurement(DataUnit.Byte).longValue(),
            List.of(2L),
            () -> 2L,
            () -> DataSize.KiB.of(100).measurement(DataUnit.Byte).longValue(),
            Map.of(),
            DataRate.KiB.of(100).perSecond());
    var tp = TopicPartition.of("test-0");
    var data = dataSupplier.apply(tp);
    Assertions.assertTrue(!data.isEmpty());
    // initial value size is 100KB and the distributed is fixed to zero, so the final size is 102400
    Assertions.assertEquals(102400, data.get(0).value().length);
  }

  @Test
  void testKeyDistribution() {
    var counter = new AtomicLong(0L);

    // Round-robin on 2 keys. Round-robin key size between 100Byte and 101Byte
    var dataSupplier =
        DataGenerator.supplier(
            1,
            0,
            0,
            List.of(0L, 1L),
            () -> counter.getAndIncrement() % 2,
            () -> 1 + ThreadLocalRandom.current().nextLong(2),
            List.of(10L),
            () -> 10L,
            () -> 10L,
            Map.of(),
            DataRate.KiB.of(200).perSecond());
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
        DataGenerator.supplier(
            1,
            0,
            0,
            List.of(0L, 1L),
            () -> counter.getAndIncrement() % 2,
            () -> 100L,
            List.of(10L),
            () -> 10L,
            () -> 10L,
            Map.of(),
            DataRate.KiB.of(200).perSecond());
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

    // Round-robin on 2 values. Round-robin value size between 100Byte and 101Byte
    var dataSupplier =
        DataGenerator.supplier(
            1,
            0,
            0,
            List.of(10L),
            () -> 10L,
            () -> 10L,
            List.of(0L, 1L),
            () -> counter.getAndIncrement() % 2,
            () -> ThreadLocalRandom.current().nextLong(2),
            Map.of(),
            DataRate.KiB.of(100).perSecond());

    var tp = TopicPartition.of("test-0");
    var data1 = dataSupplier.apply(tp);
    Assertions.assertFalse(data1.isEmpty());
    var data2 = dataSupplier.apply(tp);
    Assertions.assertFalse(data2.isEmpty());
    var data3 = dataSupplier.apply(tp);
    Assertions.assertFalse(data3.isEmpty());

    // The value of data1 and data2 should have size 0 bytes or 1 byte.
    Assertions.assertTrue(data1.get(0).value().length <= 1);
    Assertions.assertTrue(data2.get(0).value().length <= 1);
    // Round-robin value distribution with 2 possible value.
    Assertions.assertEquals(data1.get(0).value(), data3.get(0).value());

    // Round-robin on 2 values. Fixed value size.
    dataSupplier =
        DataGenerator.supplier(
            1,
            0,
            0,
            List.of(10L),
            () -> 10L,
            () -> 10L,
            List.of(0L, 1L),
            () -> counter.getAndIncrement() % 2,
            () -> 100L,
            Map.of(),
            DataRate.KiB.of(100).perSecond());
    data1 = dataSupplier.apply(tp);
    Assertions.assertTrue(!data1.isEmpty());
    data2 = dataSupplier.apply(tp);
    Assertions.assertTrue(!data2.isEmpty());
    // Same size but different content
    Assertions.assertEquals(100, data1.get(0).value().length);
    Assertions.assertEquals(100, data2.get(0).value().length);
    Assertions.assertFalse(Arrays.equals(data1.get(0).value(), data2.get(0).value()));
  }

  @Test
  void testThrottle() {
    var durationInSeconds = new AtomicLong(1);
    var throttler =
        new DataGenerator.Throttler(DataRate.KiB.of(150).perSecond()) {
          @Override
          long durationInSeconds() {
            return durationInSeconds.get();
          }
        };
    // total: 100KB, limit: 150KB -> no throttle
    Assertions.assertFalse(
        throttler.throttled(DataSize.KiB.of(100).measurement(DataUnit.Byte).longValue()));
    // total: 200KB, limit: 150KB -> throttled
    Assertions.assertTrue(
        throttler.throttled(DataSize.KiB.of(100).measurement(DataUnit.Byte).longValue()));
  }

  @Test
  void testNoKey() {
    var dataSupplier =
        DataGenerator.supplier(
            1,
            0,
            0,
            List.of(10L),
            () -> 10L,
            () -> 0L,
            List.of(10L),
            () -> 10L,
            () -> 10L,
            Map.of(),
            DataRate.KiB.of(200).perSecond());

    var tp = TopicPartition.of("test-0");
    var data = dataSupplier.apply(tp);
    Assertions.assertFalse(data.isEmpty());
    Assertions.assertNull(data.get(0).key());
  }

  @Test
  void testNoValue() {
    var dataSupplier =
        DataGenerator.supplier(
            1,
            0,
            0,
            List.of(10L),
            () -> 10L,
            () -> 10L,
            List.of(10L),
            () -> 10L,
            () -> 0L,
            Map.of(),
            DataRate.KiB.of(200).perSecond());
    var tp = TopicPartition.of("test-0");
    var data = dataSupplier.apply(tp);
    Assertions.assertFalse(data.isEmpty());
    Assertions.assertEquals(0, data.get(0).value().length);
  }

  @Test
  void testBatch() {
    var dataSupplier =
        DataGenerator.supplier(
            3,
            0,
            0,
            List.of(1L),
            () -> 1L,
            () -> 1L,
            List.of(1L),
            () -> 1L,
            () -> 1L,
            Map.of(),
            DataRate.KiB.of(100).perSecond());
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
        DataGenerator.supplier(
            10,
            keyContentSeed,
            valueContentSeed,
            LongStream.rangeClosed(0, size).boxed().collect(Collectors.toUnmodifiableList()),
            () -> Math.abs(keyRandom0.nextLong() % size),
            () -> 32L,
            LongStream.rangeClosed(0, size).boxed().collect(Collectors.toUnmodifiableList()),
            () -> Math.abs(valueRandom0.nextLong() % size),
            () -> 256L,
            Map.of(),
            DataRate.MB.of(1).perSecond());
    var gen1 =
        DataGenerator.supplier(
            10,
            keyContentSeed,
            valueContentSeed,
            LongStream.rangeClosed(0, size).boxed().collect(Collectors.toUnmodifiableList()),
            () -> Math.abs(keyRandom1.nextLong() % size),
            () -> 32L,
            LongStream.rangeClosed(0, size).boxed().collect(Collectors.toUnmodifiableList()),
            () -> Math.abs(valueRandom1.nextLong() % size),
            () -> 256L,
            Map.of(),
            DataRate.MB.of(1).perSecond());

    var tp = TopicPartition.of("A", -1);
    var series0 =
        IntStream.range(0, 10000)
            .mapToObj(i -> gen0.apply(tp))
            .collect(Collectors.toUnmodifiableList());
    var series1 =
        IntStream.range(0, 10000)
            .mapToObj(i -> gen1.apply(tp))
            .collect(Collectors.toUnmodifiableList());
    for (int i = 0; i < 10000; i++) {
      for (int j = 0; j < 10; j++) {
        Assertions.assertArrayEquals(series0.get(i).get(j).key(), series1.get(i).get(j).key());
        Assertions.assertArrayEquals(series0.get(i).get(j).value(), series1.get(i).get(j).value());
      }
    }
  }
}
