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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.astraea.common.DataRate;
import org.astraea.common.DataSize;
import org.astraea.common.DataUnit;
import org.astraea.common.admin.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DataSupplierTest {
  static Supplier<byte[]> keySupplier(
      ConcurrentHashMap<Long, byte[]> keyTable,
      Supplier<Long> keyDistribution,
      Supplier<Long> keySizeDistribution,
      Random random) {
    return () ->
        DataGenerator.getOrNew(
            keyTable, keyDistribution, keySizeDistribution.get().intValue(), random);
  }

  static Supplier<byte[]> valueSupplier(
      ConcurrentHashMap<Long, byte[]> valueTable,
      Supplier<Long> valueDistribution,
      Supplier<Long> valueSizeDistribution,
      Random random) {
    return () ->
        DataGenerator.getOrNew(
            valueTable, valueDistribution, valueSizeDistribution.get().intValue(), random);
  }

  @Test
  void testKeySize() {
    var recordKeyTable = new ConcurrentHashMap<Long, byte[]>();
    var recordValueTable = new ConcurrentHashMap<Long, byte[]>();
    var random = new Random();
    var ks =
        keySupplier(
            recordKeyTable,
            () -> 1L,
            () -> DataSize.Byte.of(20).measurement(DataUnit.Byte).longValue(),
            random);
    var vs =
        valueSupplier(
            recordValueTable,
            () -> 2L,
            () -> DataSize.KiB.of(100).measurement(DataUnit.Byte).longValue(),
            random);
    var dataSupplier =
        DataGenerator.dataSupplier(
            1,
            Map.of(),
            new DataGenerator.Throttler(DataRate.KiB.of(200).perSecond()),
            "Throttled",
            1,
            ks,
            vs);
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
    var recordKeyTable = new ConcurrentHashMap<Long, byte[]>();
    var recordValueTable = new ConcurrentHashMap<Long, byte[]>();
    var random = new Random();
    var ks =
        keySupplier(
            recordKeyTable,
            () -> 1L,
            () -> DataSize.Byte.of(20).measurement(DataUnit.Byte).longValue(),
            random);
    var vs =
        valueSupplier(
            recordValueTable,
            () -> 2L,
            () -> DataSize.KiB.of(100).measurement(DataUnit.Byte).longValue(),
            random);
    var dataSupplier =
        DataGenerator.dataSupplier(
            1,
            Map.of(),
            new DataGenerator.Throttler(DataRate.KiB.of(100).perSecond()),
            "throttled",
            1,
            ks,
            vs);
    var tp = TopicPartition.of("test-0");
    var data = dataSupplier.apply(tp);
    Assertions.assertTrue(!data.isEmpty());
    // initial value size is 100KB and the distributed is fixed to zero, so the final size is 102400
    Assertions.assertEquals(102400, data.get(0).value().length);
  }

  @Test
  void testKeyDistribution() {
    var counter = new AtomicLong(0L);
    var counter2 = new AtomicLong(0L);
    var recordKeyTable = new ConcurrentHashMap<Long, byte[]>();
    var recordValueTable = new ConcurrentHashMap<Long, byte[]>();
    var random = new Random();
    var ks =
        keySupplier(
            recordKeyTable,
            () -> counter.getAndIncrement() % 2,
            () -> 100L + counter2.getAndIncrement(),
            random);
    var vs = valueSupplier(recordValueTable, () -> 10L, () -> 10L, random);
    var dataSupplier =
        DataGenerator.dataSupplier(
            1,
            Map.of(),
            new DataGenerator.Throttler(DataRate.KiB.of(200).perSecond()),
            "throttled",
            1,
            ks,
            vs);
    var tp = TopicPartition.of("test-0");
    // Round-robin on 2 keys. Round-robin key size between 100Byte and 101Byte

    var data1 = dataSupplier.apply(tp);
    Assertions.assertTrue(!data1.isEmpty());
    var data2 = dataSupplier.apply(tp);
    Assertions.assertTrue(!data2.isEmpty());
    var data3 = dataSupplier.apply(tp);
    Assertions.assertTrue(!data3.isEmpty());

    // The key of data1 and data2 should have size 100 bytes and 101 bytes respectively.
    Assertions.assertEquals(100, data1.get(0).key().length);
    Assertions.assertEquals(101, data2.get(0).key().length);
    // Round-robin key distribution with 2 possible key.
    Assertions.assertEquals(data1.get(0).key(), data3.get(0).key());

    // Round-robin on 2 keys. Fixed key size to 100 bytes.
    var recordKeyTable2 = new ConcurrentHashMap<Long, byte[]>();
    var recordValueTable2 = new ConcurrentHashMap<Long, byte[]>();
    var ks2 = keySupplier(recordKeyTable2, () -> counter.getAndIncrement() % 2, () -> 100L, random);
    var vs2 = valueSupplier(recordValueTable2, () -> 10L, () -> 10L, random);
    dataSupplier =
        DataGenerator.dataSupplier(
            1,
            Map.of(),
            new DataGenerator.Throttler(DataRate.KiB.of(200).perSecond()),
            "throttled",
            1,
            ks2,
            vs2);

    data1 = dataSupplier.apply(tp);
    Assertions.assertTrue(!data1.isEmpty());
    data2 = dataSupplier.apply(tp);
    Assertions.assertTrue(!data2.isEmpty());
    // Same size but different content
    Assertions.assertEquals(100, data1.get(0).key().length);
    Assertions.assertEquals(100, data2.get(0).key().length);
    Assertions.assertFalse(Arrays.equals(data1.get(0).key(), data2.get(0).key()));
  }

  @Test
  void testDistributedValueSize() {
    var counter = new AtomicLong(0);
    var counter2 = new AtomicLong(0);
    var recordKeyTable = new ConcurrentHashMap<Long, byte[]>();
    var recordValueTable = new ConcurrentHashMap<Long, byte[]>();
    var random = new Random();
    var ks = keySupplier(recordKeyTable, () -> 10L, () -> 10L, random);
    var vs =
        valueSupplier(
            recordValueTable,
            () -> counter.getAndIncrement() % 2,
            () -> 100L + counter2.getAndIncrement(),
            random);
    // Round-robin on 2 values. Round-robin value size between 100Byte and 101Byte
    var dataSupplier =
        DataGenerator.dataSupplier(
            1,
            Map.of(),
            new DataGenerator.Throttler(DataRate.KiB.of(100).perSecond()),
            "throttled",
            1,
            ks,
            vs);

    var tp = TopicPartition.of("test-0");
    var data1 = dataSupplier.apply(tp);
    Assertions.assertTrue(!data1.isEmpty());
    var data2 = dataSupplier.apply(tp);
    Assertions.assertTrue(!data2.isEmpty());
    var data3 = dataSupplier.apply(tp);
    Assertions.assertTrue(!data3.isEmpty());

    // The value of data1 and data2 should have size 100 bytes and 101 bytes respectively.
    Assertions.assertEquals(100, data1.get(0).value().length);
    Assertions.assertEquals(101, data2.get(0).value().length);
    // Round-robin value distribution with 2 possible value.
    Assertions.assertEquals(data1.get(0).value(), data3.get(0).value());

    var recordKeyTable2 = new ConcurrentHashMap<Long, byte[]>();
    var recordValueTable2 = new ConcurrentHashMap<Long, byte[]>();
    var ks2 = keySupplier(recordKeyTable2, () -> 10L, () -> 10L, random);
    var vs2 =
        valueSupplier(recordValueTable2, () -> counter.getAndIncrement() % 2, () -> 100L, random);

    // Round-robin on 2 values. Fixed value size.
    dataSupplier =
        DataGenerator.dataSupplier(
            1,
            Map.of(),
            new DataGenerator.Throttler(DataRate.KiB.of(100).perSecond()),
            "throttled",
            1,
            ks2,
            vs2);
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
    var recordKeyTable = new ConcurrentHashMap<Long, byte[]>();
    var recordValueTable = new ConcurrentHashMap<Long, byte[]>();
    var random = new Random();
    var ks = keySupplier(recordKeyTable, () -> 10L, () -> 0L, random);
    var vs = valueSupplier(recordValueTable, () -> 10L, () -> 10L, random);
    var dataSupplier =
        DataGenerator.dataSupplier(
            1,
            Map.of(),
            new DataGenerator.Throttler(DataRate.KiB.of(200).perSecond()),
            "throttled",
            1,
            ks,
            vs);

    var tp = TopicPartition.of("test-0");
    var data = dataSupplier.apply(tp);
    Assertions.assertTrue(!data.isEmpty());
    Assertions.assertNull(data.get(0).key());
  }

  @Test
  void testNoValue() {
    var recordKeyTable = new ConcurrentHashMap<Long, byte[]>();
    var recordValueTable = new ConcurrentHashMap<Long, byte[]>();
    var random = new Random();
    var ks = keySupplier(recordKeyTable, () -> 10L, () -> 10L, random);
    var vs = valueSupplier(recordValueTable, () -> 10L, () -> 0L, random);
    var dataSupplier =
        DataGenerator.dataSupplier(
            1,
            Map.of(),
            new DataGenerator.Throttler(DataRate.KiB.of(200).perSecond()),
            "throttled",
            1,
            ks,
            vs);
    var tp = TopicPartition.of("test-0");
    var data = dataSupplier.apply(tp);
    Assertions.assertTrue(!data.isEmpty());
    Assertions.assertNull(data.get(0).value());
  }

  @Test
  void testBatch() {
    var recordKeyTable = new ConcurrentHashMap<Long, byte[]>();
    var recordValueTable = new ConcurrentHashMap<Long, byte[]>();
    var random = new Random();
    var ks = keySupplier(recordKeyTable, () -> 1L, () -> 1L, random);
    var vs = valueSupplier(recordValueTable, () -> 1L, () -> 1L, random);
    var dataSupplier =
        DataGenerator.dataSupplier(
            3,
            Map.of(),
            new DataGenerator.Throttler(DataRate.KiB.of(100).perSecond()),
            "throttled",
            1,
            ks,
            vs);
    var tp = TopicPartition.of("test-0");
    var data = dataSupplier.apply(tp);
    Assertions.assertEquals(3, data.size());
  }
}
