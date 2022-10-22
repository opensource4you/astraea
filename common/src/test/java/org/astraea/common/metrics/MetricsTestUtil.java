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
package org.astraea.common.metrics;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.metrics.broker.HasCount;
import org.astraea.common.metrics.broker.HasEventType;
import org.astraea.common.metrics.broker.HasGauge;
import org.astraea.common.metrics.broker.HasHistogram;
import org.astraea.common.metrics.broker.HasMeter;
import org.astraea.common.metrics.broker.HasPercentiles;
import org.astraea.common.metrics.broker.HasRate;
import org.astraea.common.metrics.broker.HasStatistics;
import org.astraea.common.metrics.broker.HasTimer;
import org.junit.jupiter.api.Assertions;

public class MetricsTestUtil {

  public static <T extends Enum<T>> boolean metricDistinct(
      T[] tEnums, Function<T, String> getMetricName) {
    var set = Arrays.stream(tEnums).map(getMetricName).collect(Collectors.toSet());
    return set.size() == tEnums.length;
  }

  public static void validate(HasBeanObject hasBeanObject) {
    if (hasBeanObject instanceof HasMeter) {
      testMeter((HasMeter) hasBeanObject);
    } else if (hasBeanObject instanceof HasTimer) {
      testTimer((HasTimer) hasBeanObject);
    } else if (hasBeanObject instanceof HasHistogram) {
      testHistogram((HasHistogram) hasBeanObject);
    } else if (hasBeanObject instanceof HasGauge) {
      testGauge((HasGauge<?>) hasBeanObject);
    } else if (hasBeanObject instanceof AppInfo) {
      testAppInfo((AppInfo) hasBeanObject);
    } else {
      throw new UnsupportedOperationException(
          String.format("Not implement. %s", hasBeanObject.getClass()));
    }
  }

  private static void testAppInfo(AppInfo appInfo) {
    Assertions.assertNotNull(appInfo.id());
    Assertions.assertNotNull(appInfo.version());
    Assertions.assertNotNull(appInfo.commitId());
    Assertions.assertTrue(appInfo.startTimeMs() > 0);
  }

  private static void testGauge(HasGauge<?> hasGauge) {
    Assertions.assertDoesNotThrow(hasGauge::value);
  }

  private static void testMeter(HasMeter hasMeter) {
    testEventType(hasMeter);
    testRate(hasMeter);
    testCount(hasMeter);
  }

  private static void testHistogram(HasHistogram hasHistogram) {
    testStatistics(hasHistogram);
    testPercentile(hasHistogram);
    testCount(hasHistogram);
  }

  private static void testTimer(HasTimer hasTimer) {
    testPercentile(hasTimer);
    testStatistics(hasTimer);
    testEventType(hasTimer);
    testRate(hasTimer);
    testCount(hasTimer);
    Assertions.assertDoesNotThrow(hasTimer::latencyUnit);
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
