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
package org.astraea.common.metrics.stats;

import java.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WindowedRateTest {
  @Test
  void testMeasure() throws InterruptedException {
    var windowRate = new WindowedRate(Duration.ofMillis(100));

    Assertions.assertEquals(0.0, windowRate.measure());

    windowRate.record(10.0);
    Assertions.assertEquals(Double.POSITIVE_INFINITY, windowRate.measure());
    Thread.sleep(Duration.ofMillis(200).toMillis());
    Assertions.assertEquals(0.0, windowRate.measure());

    windowRate.record(5.0);
    // 10.0 => 15.0, increased 50%
    Assertions.assertEquals(0.5, windowRate.measure());

    Thread.sleep(Duration.ofMillis(60).toMillis());

    windowRate.record(15.0);
    // 10.0 => 30.0, increased 200%
    Assertions.assertEquals(2.0, windowRate.measure());

    Thread.sleep(Duration.ofMillis(60).toMillis());
    // 15.0 => 30.0
    Assertions.assertEquals(1.0, windowRate.measure());
  }

  @Test
  void testNegativeRecord() throws InterruptedException {
    var windowRate = new WindowedRate(Duration.ofMillis(100));

    windowRate.record(32.0);
    Thread.sleep(Duration.ofMillis(200).toMillis());

    Assertions.assertEquals(0.0, windowRate.measure());
    windowRate.record(-16.0);
    Assertions.assertEquals(-0.5, windowRate.measure());
  }
}
