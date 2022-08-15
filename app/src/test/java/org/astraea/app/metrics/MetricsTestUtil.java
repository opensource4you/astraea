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
package org.astraea.app.metrics;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.app.metrics.broker.HasCount;
import org.astraea.app.metrics.broker.HasEventType;
import org.astraea.app.metrics.broker.HasPercentiles;
import org.astraea.app.metrics.broker.HasRate;
import org.astraea.app.metrics.broker.HasStatistics;
import org.astraea.app.metrics.broker.IsMeter;
import org.astraea.app.metrics.broker.IsTimer;
import org.junit.jupiter.api.Assertions;

public class MetricsTestUtil {

  public static <T extends Enum<T>> boolean metricDistinct(
      T[] tEnums, Function<T, String> getMetricName) {
    var set = Arrays.stream(tEnums).map(getMetricName).collect(Collectors.toSet());
    return set.size() == tEnums.length;
  }

  public static void testMeter(IsMeter isMeter) {
    testEventType(isMeter);
    testRate(isMeter);
    testCount(isMeter);
  }

  public static void testTimer(IsTimer isTimer) {
    testPercentile(isTimer);
    testStatistics(isTimer);
    testEventType(isTimer);
    testRate(isTimer);
    testCount(isTimer);
    Assertions.assertDoesNotThrow(isTimer::latencyUnit);
  }

  private static void testStatistics(HasStatistics hasStatistics) {
    Assertions.assertDoesNotThrow(hasStatistics::max);
    Assertions.assertDoesNotThrow(hasStatistics::mean);
    Assertions.assertDoesNotThrow(hasStatistics::min);
    Assertions.assertDoesNotThrow(hasStatistics::stdDev);
  }

  private static void testPercentile(HasPercentiles hasPercentiles) {
    Assertions.assertDoesNotThrow(hasPercentiles::percentile50);
    Assertions.assertDoesNotThrow(hasPercentiles::percentile75);
    Assertions.assertDoesNotThrow(hasPercentiles::percentile95);
    Assertions.assertDoesNotThrow(hasPercentiles::percentile98);
    Assertions.assertDoesNotThrow(hasPercentiles::percentile99);
    Assertions.assertDoesNotThrow(hasPercentiles::percentile999);
  }

  private static void testEventType(HasEventType hasEventType) {
    Assertions.assertDoesNotThrow(hasEventType::eventType);
  }

  private static void testRate(HasRate hasRate) {
    Assertions.assertDoesNotThrow(hasRate::fifteenMinuteRate);
    Assertions.assertDoesNotThrow(hasRate::fiveMinuteRate);
    Assertions.assertDoesNotThrow(hasRate::meanRate);
    Assertions.assertDoesNotThrow(hasRate::oneMinuteRate);
    Assertions.assertDoesNotThrow(hasRate::rateUnit);
  }

  private static void testCount(HasCount hasCount) {
    Assertions.assertTrue(hasCount.count() >= 0);
  }
}
