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
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OnceByPeriodTest {
  @Test
  void testRecord() {
    var rangedDataCalculator = new OnceByPeriod<Double>(Duration.ofMillis(500));
    Assertions.assertEquals(Optional.of(20.0), rangedDataCalculator.record(20.0, 100));
    Assertions.assertEquals(Optional.empty(), rangedDataCalculator.record(21.0, 110));
    Assertions.assertEquals(Optional.of(60.0), rangedDataCalculator.record(60.0, 601));
  }
}
