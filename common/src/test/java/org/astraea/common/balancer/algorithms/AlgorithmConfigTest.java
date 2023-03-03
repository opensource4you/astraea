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
package org.astraea.common.balancer.algorithms;

import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.balancer.AlgorithmConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AlgorithmConfigTest {

  @Test
  void testCopy() {
    var config0 =
        AlgorithmConfig.builder()
            .clusterInfo(ClusterInfo.empty())
            .clusterBean(ClusterBean.EMPTY)
            .clusterCost((i, j) -> () -> 100)
            .build();
    var config1 = AlgorithmConfig.builder(config0).build();
    Assertions.assertSame(config0.executionId(), config1.executionId());
    Assertions.assertSame(config0.clusterCostFunction(), config1.clusterCostFunction());
    Assertions.assertSame(config0.moveCostFunction(), config1.moveCostFunction());
    Assertions.assertSame(config0.clusterConstraint(), config1.clusterConstraint());
    Assertions.assertSame(config0.topicFilter(), config1.topicFilter());
    Assertions.assertSame(config0.clusterInfo(), config1.clusterInfo());
    Assertions.assertSame(config0.clusterBean(), config1.clusterBean());
    Assertions.assertSame(config0.timeout(), config1.timeout());
  }
}
