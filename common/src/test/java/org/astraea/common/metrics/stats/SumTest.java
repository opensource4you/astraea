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
import org.astraea.common.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SumTest {
  @Test
  void testLongOf() {
    var stat = Sum.ofLong();
    stat.record(1L);
    stat.record(2L);
    Assertions.assertEquals(3L, stat.measure());
  }

  @Test
  void testLongByTime() {
    var stat = Sum.longByTime(Duration.ofMillis(20));

    stat.record(1L);
    // 1L: just now
    Assertions.assertEquals(1L, stat.measure());

    Utils.sleep(Duration.ofMillis(10));

    stat.record(2L);
    // 1L: 10 milliseconds ago
    // 2L: just now
    Assertions.assertEquals(3L, stat.measure());

    Utils.sleep(Duration.ofMillis(15));
    // 1L: 25 milliseconds ago (outdated)
    // 2L: 15 milliseconds ago
    Assertions.assertEquals(2L, stat.measure());

    Utils.sleep(Duration.ofMillis(10));
    // 2L: 25 milliseconds ago (outdated)
    Assertions.assertEquals(0L, stat.measure());
  }
}
