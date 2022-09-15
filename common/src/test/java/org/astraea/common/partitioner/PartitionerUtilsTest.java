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
package org.astraea.common.partitioner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.astraea.common.cost.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionerUtilsTest {

  @Test
  void testFactorial() {
    Assertions.assertEquals(PartitionerUtils.factorial(3), 6);
    Assertions.assertEquals(PartitionerUtils.factorial(5), 120);
  }

  @Test
  void testDoPoisson() {
    Assertions.assertEquals(PartitionerUtils.doPoisson(10, 15), 0.9512595966960214);
    Assertions.assertEquals(PartitionerUtils.doPoisson(5, 5), 0.6159606548330632);
    Assertions.assertEquals(PartitionerUtils.doPoisson(5, 8), 0.9319063652781515);
    Assertions.assertEquals(PartitionerUtils.doPoisson(5, 2), 0.12465201948308113);
  }

  @Test
  void testSetAllPoisson() {
    HashMap<Integer, Integer> testNodesLoadCount = new HashMap<>();
    testNodesLoadCount.put(0, 2);
    testNodesLoadCount.put(1, 5);
    testNodesLoadCount.put(2, 8);

    Map<Integer, Double> poissonMap;
    HashMap<Integer, Double> testPoissonMap = new HashMap<>();
    testPoissonMap.put(0, 0.12465201948308113);
    testPoissonMap.put(1, 0.6159606548330632);
    testPoissonMap.put(2, 0.9319063652781515);

    poissonMap = PartitionerUtils.allPoisson(testNodesLoadCount);

    assertEquals(poissonMap, testPoissonMap);
  }

  @Test
  void testWeightPoisson() {
    Assertions.assertEquals(PartitionerUtils.weightPoisson(0.5, 1.0), 10);
    Assertions.assertEquals(PartitionerUtils.weightPoisson(1.0, 1.0), 0);
    Assertions.assertEquals(PartitionerUtils.weightPoisson(0.95, 1.0), 0);
    Assertions.assertEquals(PartitionerUtils.weightPoisson(0.9, 1.0), 0);
    Assertions.assertEquals(PartitionerUtils.weightPoisson(0.0, 1.0), 20);
    Assertions.assertEquals(PartitionerUtils.weightPoisson(0.8, 1.0), 1);
  }

  @Test
  void testParseIdJMXPort() {
    var config =
        Configuration.of(Map.of("broker.1001.jmx.port", "8000", "broker.1002.jmx.port", "8001"));
    var ans = PartitionerUtils.parseIdJMXPort(config);
    Assertions.assertEquals(2, ans.size());
    Assertions.assertEquals(8000, ans.get(1001));
    Assertions.assertEquals(8001, ans.get(1002));

    config = Configuration.of(Map.of("jmx.port", "8000", "broker.1002.jmx.port", "8001"));
    ans = PartitionerUtils.parseIdJMXPort(config);
    Assertions.assertEquals(1, ans.size());
    Assertions.assertEquals(8001, ans.get(1002));

    var config3 = Configuration.of(Map.of("broker.id.jmx.port", "8000"));
    Assertions.assertThrows(
        NumberFormatException.class, () -> PartitionerUtils.parseIdJMXPort(config3));
  }
}
