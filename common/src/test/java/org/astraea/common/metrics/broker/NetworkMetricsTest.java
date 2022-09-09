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
package org.astraea.common.metrics.broker;

import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.MetricsTestUtil;
import org.astraea.it.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class NetworkMetricsTest extends RequireSingleBrokerCluster {

  @ParameterizedTest()
  @EnumSource(value = NetworkMetrics.Request.class)
  void testRequestTotalTimeMs(NetworkMetrics.Request request) {
    var histogram = request.fetch(MBeanClient.local());
    MetricsTestUtil.validate(histogram);
  }

  @Test
  void testAllEnumNameUnique() {
    Assertions.assertTrue(
        MetricsTestUtil.metricDistinct(
            NetworkMetrics.Request.values(), NetworkMetrics.Request::metricName));
  }
}
