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

public class AvgTest {
  @Test
  void testAvg() {
    var stat = Avg.of();
    stat.record(2.0);
    stat.record(7.0);
    stat.record(6.0);

    Assertions.assertEquals(5.0, stat.measure());
  }

  @Test
  void testException() {
    var stat = Avg.of();
    Assertions.assertThrows(RuntimeException.class, stat::measure);
  }

  @Test
  void testRateByTime() throws InterruptedException {
    var rateByTime = Avg.rateByTime(Duration.ofSeconds(1));
    rateByTime.record(10.0);
    rateByTime.record(10.0);
    Thread.sleep(1000);
    rateByTime.record(50.0);

    Assertions.assertEquals((10 + 50) / 2.0, rateByTime.measure());

    rateByTime.record(50.0);

    Assertions.assertEquals((10 + 50) / 2.0, rateByTime.measure());
  }

  @Test
  void testExpWeightByTime() throws InterruptedException {
    var rateByTime = Avg.expWeightByTime(Duration.ofSeconds(1), 0.5);
    rateByTime.record(10.0);
    rateByTime.record(10.0);
    Thread.sleep(1000);
    rateByTime.record(50.0);

    Assertions.assertEquals(10 * 0.5 * 0.5 + 50 * 0.5, rateByTime.measure());

    rateByTime.record(50.0);

    Assertions.assertEquals(10 * 0.5 * 0.5 + 50 * 0.5, rateByTime.measure());
  }
}
