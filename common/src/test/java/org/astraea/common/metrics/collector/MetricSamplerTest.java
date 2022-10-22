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
package org.astraea.common.metrics.collector;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.metrics.platform.HostMetrics;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class MetricSamplerTest extends RequireBrokerCluster {

  @Test
  void offer() {
    var local =
        InetSocketAddress.createUnresolved(jmxServiceURL().getHost(), jmxServiceURL().getPort());
    var interval = Duration.ofMillis(1000);
    var fetcher = (Fetcher) (client) -> List.of(HostMetrics.jvmMemory(client));
    try (var metricSampler =
        new MetricSampler(fetcher, interval, Map.of(0, local, 1, local, 2, local))) {
      Utils.sleep(interval.plusMillis(500));
      ClusterBean offer = metricSampler.offer();
      Assertions.assertEquals(1, offer.all().get(0).size());
      Assertions.assertEquals(1, offer.all().get(1).size());
      Assertions.assertEquals(1, offer.all().get(2).size());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1000, 2500, 3456})
  void interval(int sleepMs) {
    var local =
        InetSocketAddress.createUnresolved(jmxServiceURL().getHost(), jmxServiceURL().getPort());
    var interval = Duration.ofMillis(sleepMs);
    var fetcher = (Fetcher) (client) -> List.of(HostMetrics.jvmMemory(client));
    try (var metricSampler =
        new MetricSampler(fetcher, interval, Map.of(0, local, 1, local, 2, local))) {
      Utils.sleep(interval.plusMillis(500));
      ClusterBean offer1 = metricSampler.offer();
      Assertions.assertEquals(1, offer1.all().get(0).size());
      Assertions.assertEquals(1, offer1.all().get(1).size());
      Assertions.assertEquals(1, offer1.all().get(2).size());
      Utils.sleep(interval.plusMillis(500));
      ClusterBean offer2 = metricSampler.offer();
      Assertions.assertEquals(2, offer2.all().get(0).size());
      Assertions.assertEquals(2, offer2.all().get(1).size());
      Assertions.assertEquals(2, offer2.all().get(2).size());
      Utils.sleep(interval.plusMillis(500));
      ClusterBean offer3 = metricSampler.offer();
      Assertions.assertEquals(3, offer3.all().get(0).size());
      Assertions.assertEquals(3, offer3.all().get(1).size());
      Assertions.assertEquals(3, offer3.all().get(2).size());
    }
  }
}
