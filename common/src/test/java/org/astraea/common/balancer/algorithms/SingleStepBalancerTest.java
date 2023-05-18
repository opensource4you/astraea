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

import java.util.Map;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.balancer.BalancerConfigTestSuite;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SingleStepBalancerTest extends BalancerConfigTestSuite {

  public SingleStepBalancerTest() {
    super(
        SingleStepBalancer.class,
        new Configuration(
            Map.of(
                "shuffle.tweaker.min.step", "1000",
                "shuffle.tweaker.max.step", "2000")));
  }

  @Test
  void testConfig() {
    Assertions.assertTrue(
        SingleStepBalancer.ALL_CONFIGS.contains("shuffle.tweaker.min.step"),
        "Config exists for backward compatability reason");
    Assertions.assertTrue(
        SingleStepBalancer.ALL_CONFIGS.contains("shuffle.tweaker.max.step"),
        "Config exists for backward compatability reason");
    Assertions.assertTrue(
        SingleStepBalancer.ALL_CONFIGS.contains("iteration"),
        "Config exists for backward compatability reason");

    Assertions.assertEquals(
        SingleStepBalancer.ALL_CONFIGS.size(),
        Utils.constants(SingleStepBalancer.class, name -> name.endsWith("CONFIG"), String.class)
            .size(),
        "No duplicate element");
  }
}
