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

public class RateByTimeTest {
  @Test
  void testMeasure() throws InterruptedException {
    var rateByTime = new RateByTime(Duration.ofSeconds(1));
    rateByTime.record(10.0);
    rateByTime.record(10.0);
    Thread.sleep(1000);
    rateByTime.record(50.0);

    Assertions.assertEquals(40.0, rateByTime.measure());

    rateByTime.record(50.0);

    Assertions.assertEquals(40.0, rateByTime.measure());
  }
}
