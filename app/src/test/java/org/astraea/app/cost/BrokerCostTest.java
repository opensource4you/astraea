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
package org.astraea.app.cost;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BrokerCostTest {
  @Test
  void testNormalize() {
    var testMap =
        IntStream.range(0, 100).boxed().collect(Collectors.toMap(i -> i, i -> i * 2 + 0.0));
    var oldKey = -1;
    var oldValue = -1.0;
    brokerCost(testMap)
        .normalize(Normalizer.TScore())
        .value()
        .forEach(
            (key, value) -> {
              Assertions.assertTrue(key > oldKey);
              Assertions.assertTrue(value > oldValue);
            });
  }

  BrokerCost brokerCost(Map<Integer, Double> map) {
    return () -> map;
  }
}
