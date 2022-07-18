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

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import org.astraea.app.common.DataUnit;
import org.astraea.app.common.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DataSupplierTest {

  @Test
  void testDuration() {
    var dataSupplier =
        DataSupplier.of(
            ExeTime.of("2s"), () -> 1L, () -> 4L, () -> 1L, () -> 10L, DataUnit.KiB.of(100));
    Assertions.assertTrue(dataSupplier.get().hasData());
    Utils.sleep(Duration.ofSeconds(3));
    Assertions.assertFalse(dataSupplier.get().hasData());
  }

  @Test
  void testRecordLimit() {
    var dataSupplier =
        DataSupplier.of(
            ExeTime.of("2records"),
            () -> DataUnit.KiB.of(2).measurement(DataUnit.Byte).longValue(),
            () -> 10L,
            () -> DataUnit.KiB.of(100).measurement(DataUnit.Byte).longValue(),
            () -> 10L,
            DataUnit.KiB.of(102));
    Assertions.assertTrue(dataSupplier.get().hasData());
    Assertions.assertTrue(dataSupplier.get().hasData());
    Assertions.assertFalse(dataSupplier.get().hasData());
  }

  @Test
  void testKeySize() {
    var dataSupplier =
        DataSupplier.of(
            ExeTime.of("10s"),
            () -> 1L,
            () -> DataUnit.Byte.of(20).measurement(DataUnit.Byte).longValue(),
            () -> 2L,
            () -> DataUnit.KiB.of(100).measurement(DataUnit.Byte).longValue(),
            DataUnit.KiB.of(200));
    var data1 = dataSupplier.get();
    Assertions.assertTrue(data1.hasData());
    var data2 = dataSupplier.get();
    Assertions.assertTrue(data2.hasData());
    // key content is fixed, the keys are the same
    Assertions.assertEquals(data1.key(), data2.key());
  }

  @Test
  void testFixedValueSize() {
    var dataSupplier =
        DataSupplier.of(
            ExeTime.of("10s"),
            () -> 1L,
            () -> DataUnit.Byte.of(20).measurement(DataUnit.Byte).longValue(),
            () -> 2L,
            () -> DataUnit.KiB.of(100).measurement(DataUnit.Byte).longValue(),
            DataUnit.KiB.of(100));
    var data = dataSupplier.get();
    Assertions.assertTrue(data.hasData());
    // initial value size is 100KB and the distributed is fixed to zero, so the final size is 102400
    Assertions.assertEquals(102400, data.value().length);
  }

  @Test
  void testKeyDistribution() {
    var counter = new AtomicLong(0L);
    var counter2 = new AtomicLong(0L);

    // Round-robin on 2 keys. Round-robin key size between 100Byte and 101Byte
    var dataSupplier =
        DataSupplier.of(
            ExeTime.of("10s"),
            () -> counter.getAndIncrement() % 2,
            () -> 100L + counter2.getAndIncrement(),
            () -> 10L,
            () -> 10L,
            DataUnit.KiB.of(200));
    var data1 = dataSupplier.get();
    Assertions.assertTrue(data1.hasData());
    var data2 = dataSupplier.get();
    Assertions.assertTrue(data2.hasData());
    var data3 = dataSupplier.get();
    Assertions.assertTrue(data3.hasData());

    // The key of data1 and data2 should have size 100 bytes and 101 bytes respectively.
    Assertions.assertEquals(100, data1.key().length);
    Assertions.assertEquals(101, data2.key().length);
    // Round-robin key distribution with 2 possible key.
    Assertions.assertEquals(data1.key(), data3.key());

    // Round-robin on 2 keys. Fixed key size to 100 bytes.
    dataSupplier =
        DataSupplier.of(
            ExeTime.of("10s"),
            () -> counter.getAndIncrement() % 2,
            () -> 100L,
            () -> 10L,
            () -> 10L,
            DataUnit.KiB.of(200));
    data1 = dataSupplier.get();
    Assertions.assertTrue(data1.hasData());
    data2 = dataSupplier.get();
    Assertions.assertTrue(data2.hasData());
    // Same size but different content
    Assertions.assertEquals(100, data1.key().length);
    Assertions.assertEquals(100, data2.key().length);
    Assertions.assertFalse(Arrays.equals(data1.key(), data2.key()));
  }

  @Test
  void testDistributedValueSize() {
    var counter = new AtomicLong(0);
    var counter2 = new AtomicLong(0);

    // Round-robin on 2 values. Round-robin value size between 100Byte and 101Byte
    var dataSupplier =
        DataSupplier.of(
            ExeTime.of("10s"),
            () -> 10L,
            () -> 10L,
            () -> counter.getAndIncrement() % 2,
            () -> 100L + counter2.getAndIncrement(),
            DataUnit.KiB.of(100));
    var data1 = dataSupplier.get();
    Assertions.assertTrue(data1.hasData());
    var data2 = dataSupplier.get();
    Assertions.assertTrue(data2.hasData());
    var data3 = dataSupplier.get();
    Assertions.assertTrue(data3.hasData());

    // The value of data1 and data2 should have size 100 bytes and 101 bytes respectively.
    Assertions.assertEquals(100, data1.value().length);
    Assertions.assertEquals(101, data2.value().length);
    // Round-robin value distribution with 2 possible value.
    Assertions.assertEquals(data1.value(), data3.value());

    // Round-robin on 2 values. Fixed value size.
    dataSupplier =
        DataSupplier.of(
            ExeTime.of("10s"),
            () -> 10L,
            () -> 10L,
            () -> counter.getAndIncrement() % 2,
            () -> 100L,
            DataUnit.KiB.of(100));
    data1 = dataSupplier.get();
    Assertions.assertTrue(data1.hasData());
    data2 = dataSupplier.get();
    Assertions.assertTrue(data2.hasData());
    // Same size but different content
    Assertions.assertEquals(100, data1.value().length);
    Assertions.assertEquals(100, data2.value().length);
    Assertions.assertFalse(Arrays.equals(data1.value(), data2.value()));
  }

  @Test
  void testThrottle() {
    var dataSupplier =
        DataSupplier.of(
            ExeTime.of("10s"),
            () -> 10L,
            () -> DataUnit.KiB.of(50).measurement(DataUnit.Byte).longValue(),
            () -> 10L,
            () -> DataUnit.KiB.of(50).measurement(DataUnit.Byte).longValue(),
            DataUnit.KiB.of(150));
    // total: 100KB, limit: 150KB -> no throttle
    Assertions.assertTrue(dataSupplier.get().hasData());
    // total: 200KB, limit: 150KB -> will throttle next data
    Assertions.assertTrue(dataSupplier.get().hasData());
    // throttled
    Assertions.assertFalse(dataSupplier.get().hasData());
  }

  @Test
  void testNoKey() {
    var dataSupplier =
        DataSupplier.of(
            ExeTime.of("10s"), () -> 10L, () -> 0L, () -> 10L, () -> 10L, DataUnit.KiB.of(200));

    var data = dataSupplier.get();
    Assertions.assertTrue(data.hasData());
    Assertions.assertNull(data.key());
  }

  @Test
  void testNoValue() {
    var dataSupplier =
        DataSupplier.of(
            ExeTime.of("10s"), () -> 10L, () -> 10L, () -> 10L, () -> 0L, DataUnit.KiB.of(200));
    var data = dataSupplier.get();
    Assertions.assertTrue(data.hasData());
    Assertions.assertNull(data.value());
  }
}
