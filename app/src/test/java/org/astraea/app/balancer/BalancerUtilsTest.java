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
package org.astraea.app.balancer;

import java.util.Map;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.partitioner.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BalancerUtilsTest {

  public static class TestConfigCostFunction implements CostFunction {
    public TestConfigCostFunction(Configuration configuration) {}
  }

  public static class TestCostFunction implements CostFunction {
    public TestCostFunction() {}
  }

  public static class TestBadCostFunction implements CostFunction {
    public TestBadCostFunction(int value) {}
  }

  @ParameterizedTest
  @ValueSource(classes = {TestCostFunction.class, TestConfigCostFunction.class})
  void constructCostFunction(Class<? extends CostFunction> aClass) {
    // arrange
    var config = Configuration.of(Map.of());

    // act
    var costFunction = BalancerUtils.constructCostFunction(aClass, config);

    // assert
    Assertions.assertInstanceOf(CostFunction.class, costFunction);
    Assertions.assertInstanceOf(aClass, costFunction);
  }

  @Test
  void constructCostFunctionException() {
    // arrange
    var aClass = TestBadCostFunction.class;
    var config = Configuration.of(Map.of());

    // act, assert
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> BalancerUtils.constructCostFunction(aClass, config));
  }
}
