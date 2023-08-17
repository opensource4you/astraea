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
package org.astraea.common.balancer;

import java.util.Arrays;
import java.util.List;
import org.astraea.common.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opentest4j.AssertionFailedError;

class BalancerConfigsTest {

  @ParameterizedTest
  @ValueSource(classes = BalancerConfigs.class)
  void testBalancerConfigPrefix(Class<?> aClass) {
    record Field<T>(String name, T value) {}

    var badFields =
        Arrays.stream(aClass.getDeclaredFields())
            .map(x -> new Field<>(x.getName(), Utils.packException(() -> x.get(null))))
            .filter(x -> x.value instanceof String configKey && !configKey.startsWith("balancer."))
            .toList();

    Assertions.assertEquals(
        List.of(),
        badFields,
        "The class '%s' contains bad config key pattern, it should start with 'balancer.'"
            .formatted(aClass));
  }

  @Test
  void testPrefixCheckCorrect() {
    @SuppressWarnings("unused")
    interface CorrectClass {
      String correct0 = "balancer.good.config.A";
      String correct1 = "balancer.good.config.B";
      String correct2 = "balancer.good.config.C";
    }

    @SuppressWarnings("unused")
    interface IncorrectClass {
      String correct0 = "balancer.good.config.A";
      String incorrect1 = "not.balancer.good.config.B";
      String incorrect2 = "not.balancer.good.config.C";
    }

    Assertions.assertDoesNotThrow(() -> testBalancerConfigPrefix(CorrectClass.class));
    Assertions.assertThrows(
        AssertionFailedError.class, () -> testBalancerConfigPrefix(IncorrectClass.class));
  }
}
