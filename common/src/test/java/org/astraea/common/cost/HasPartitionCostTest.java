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
package org.astraea.common.cost;

import java.util.Map;
import java.util.Optional;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.collector.MetricSensor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class HasPartitionCostTest {

  @Test
  void testSensor() {
    var sensor = Mockito.mock(MetricSensor.class);
    var function =
        new HasPartitionCost() {

          @Override
          public PartitionCost partitionCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
            return () -> Map.of(TopicPartition.of("a", 1), 1D);
          }

          @Override
          public Optional<MetricSensor> metricSensor() {
            return Optional.of(sensor);
          }
        };

    var f2 = HasPartitionCost.of(Map.of(function, 1D));
    Assertions.assertTrue(f2.metricSensor().isPresent());
  }
}
