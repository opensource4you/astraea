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
package org.astraea.app.metrics.broker;

import org.astraea.app.metrics.MBeanClient;
import org.astraea.app.metrics.MetricsTestUtil;
import org.astraea.app.service.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class NetworkMetricsTest extends RequireSingleBrokerCluster {

  @ParameterizedTest()
  @EnumSource(value = NetworkMetrics.Request.class)
  void testRequestTotalTimeMs(NetworkMetrics.Request request) {
    // act
    var totalTimeMs = request.totalTimeMs(MBeanClient.local());

    // assert type casting correct and field exists
    Assertions.assertDoesNotThrow(totalTimeMs::percentile50);
    Assertions.assertDoesNotThrow(totalTimeMs::percentile75);
    Assertions.assertDoesNotThrow(totalTimeMs::percentile95);
    Assertions.assertDoesNotThrow(totalTimeMs::percentile98);
    Assertions.assertDoesNotThrow(totalTimeMs::percentile99);
    Assertions.assertDoesNotThrow(totalTimeMs::percentile999);
    Assertions.assertDoesNotThrow(totalTimeMs::count);
    Assertions.assertDoesNotThrow(totalTimeMs::max);
    Assertions.assertDoesNotThrow(totalTimeMs::mean);
    Assertions.assertDoesNotThrow(totalTimeMs::min);
    Assertions.assertDoesNotThrow(totalTimeMs::stdDev);
    Assertions.assertEquals(request, totalTimeMs.type());
  }

  @Test
  void testAllEnumNameUnique() {
    Assertions.assertTrue(
        MetricsTestUtil.metricDistinct(
            NetworkMetrics.Request.values(), NetworkMetrics.Request::metricName));
  }
}
