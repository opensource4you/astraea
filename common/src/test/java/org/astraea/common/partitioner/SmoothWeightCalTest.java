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

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SmoothWeightCalTest {
  @Test
  void testRefresh() {
    var smoothWeight = new SmoothWeightCal<>(Map.of(1, 5.0, 2, 3.0, 3, 1.0));

    var effectiveWeightResult = smoothWeight.effectiveWeightResult.get();
    Assertions.assertEquals(1.0, effectiveWeightResult.get(1));
    Assertions.assertEquals(1.0, effectiveWeightResult.get(2));
    Assertions.assertEquals(1.0, effectiveWeightResult.get(3));

    smoothWeight.refresh(() -> Map.of(1, 6.0, 2, 3.0, 3, 6.0));
    effectiveWeightResult = smoothWeight.effectiveWeightResult.get();

    Assertions.assertEquals(0.8, effectiveWeightResult.get(1));
    Assertions.assertEquals(1.4, effectiveWeightResult.get(2));
    Assertions.assertEquals(0.8, effectiveWeightResult.get(3));

    smoothWeight.refresh(() -> Map.of(1, 10.0, 2, 1.0, 3, 10.0));
    effectiveWeightResult = smoothWeight.effectiveWeightResult.get();

    Assertions.assertEquals(0.45714285714285713, effectiveWeightResult.get(1));
    Assertions.assertEquals(2.6, effectiveWeightResult.get(2));
    Assertions.assertEquals(0.45714285714285713, effectiveWeightResult.get(3));
  }

  @Test
  void testLazyEffectiveWeightResult() {
    var smoothWeight = new SmoothWeightCal<>(Map.of(1, 5.0, 2, 3.0, 3, 1.0));

    var effectiveWeightResult = smoothWeight.effectiveWeightResult.get();
    Assertions.assertEquals(1.0, effectiveWeightResult.get(1));
    Assertions.assertEquals(1.0, effectiveWeightResult.get(2));
    Assertions.assertEquals(1.0, effectiveWeightResult.get(3));

    smoothWeight.refresh(() -> Map.of(1, 10.0, 2, 3.0, 3, 10.0));
    smoothWeight.refresh(() -> Map.of(1, 6.0, 2, 3.0, 3, 6.0));
    effectiveWeightResult = smoothWeight.effectiveWeightResult.get();

    Assertions.assertEquals(0.8, effectiveWeightResult.get(1));
    Assertions.assertEquals(1.4, effectiveWeightResult.get(2));
    Assertions.assertEquals(0.8, effectiveWeightResult.get(3));
  }
}
