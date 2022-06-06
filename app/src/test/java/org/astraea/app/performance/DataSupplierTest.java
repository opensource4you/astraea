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

import java.util.concurrent.TimeUnit;
import org.astraea.app.common.DataUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DataSupplierTest {

  @Test
  void testDuration() throws InterruptedException {
    var dataSupplier =
        DataSupplier.of(
            ExeTime.of("2s"),
            DistributionType.FIXED.create(10),
            DataUnit.KiB.of(100),
            DistributionType.FIXED.create(10),
            DataUnit.KiB.of(100));
    Assertions.assertTrue(dataSupplier.get().hasData());
    TimeUnit.SECONDS.sleep(3);
    Assertions.assertFalse(dataSupplier.get().hasData());
  }

  @Test
  void testRecordLimit() {
    var dataSupplier =
        DataSupplier.of(
            ExeTime.of("2records"),
            DistributionType.FIXED.create(10),
            DataUnit.KiB.of(100),
            DistributionType.FIXED.create(10),
            DataUnit.KiB.of(100));
    Assertions.assertTrue(dataSupplier.get().hasData());
    Assertions.assertTrue(dataSupplier.get().hasData());
    Assertions.assertFalse(dataSupplier.get().hasData());
  }

  @Test
  void testKeySize() {
    var dataSupplier =
        DataSupplier.of(
            ExeTime.of("10s"),
            DistributionType.FIXED.create(9),
            DataUnit.KiB.of(100),
            DistributionType.FIXED.create(10),
            DataUnit.KiB.of(100));
    var data = dataSupplier.get();
    Assertions.assertTrue(data.hasData());
    // key content is fixed to "9", so the size is 1 byte
    Assertions.assertEquals(1, data.key().length);
  }

  @Test
  void testFixedValueSize() {
    var dataSupplier =
        DataSupplier.of(
            ExeTime.of("10s"),
            DistributionType.FIXED.create(10),
            DataUnit.KiB.of(100),
            DistributionType.FIXED.create(0),
            DataUnit.KiB.of(100));
    var data = dataSupplier.get();
    Assertions.assertTrue(data.hasData());
    // initial value size is 100KB and the distributed is fixed to zero, so the final size is 102400
    Assertions.assertEquals(102400, data.value().length);
  }

  @Test
  void testDistributedValueSize() {
    var dataSupplier =
        DataSupplier.of(
            ExeTime.of("10s"),
            DistributionType.FIXED.create(10),
            DataUnit.KiB.of(100),
            DistributionType.FIXED.create(10),
            DataUnit.KiB.of(100));
    var data = dataSupplier.get();
    Assertions.assertTrue(data.hasData());
    // initial value size is 100KB and the distributed is fixed to 10, so the final size is between
    // (102400 - 10, 102400 + 10)
    Assertions.assertTrue(data.value().length >= 102400 - 10 && data.value().length <= 102400 + 10);
  }

  @Test
  void testThrottle() {
    var dataSupplier =
        DataSupplier.of(
            ExeTime.of("10s"),
            DistributionType.FIXED.create(10),
            DataUnit.KiB.of(100),
            DistributionType.FIXED.create(0),
            DataUnit.KiB.of(150));
    // total: 100KB, limit: 150KB -> no throttle
    Assertions.assertTrue(dataSupplier.get().hasData());
    // total: 200KB, limit: 150KB -> will throttle next data
    Assertions.assertTrue(dataSupplier.get().hasData());
    // throttled
    Assertions.assertFalse(dataSupplier.get().hasData());
  }
}
