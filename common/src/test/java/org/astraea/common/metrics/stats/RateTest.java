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

public class RateTest {

  @Test
  void testCountMeasure() {
    var rate = Rate.of();
    Assertions.assertDoesNotThrow(rate::measure);
    Assertions.assertEquals(0.0, rate.measure());
    rate.record(1.0);
    rate.record(1.0);
    Utils.sleep(Duration.ofSeconds(1));
    Assertions.assertTrue(rate.measure() < 2);
    Assertions.assertTrue(rate.measure() > 1.5);
  }
}
