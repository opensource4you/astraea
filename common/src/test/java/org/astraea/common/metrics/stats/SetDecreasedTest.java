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
package org.astraea.common.metrics.stats;

import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SetDecreasedTest {
  @Test
  void testMeasure() {
    var setDecreased = new SetDecreased<>(2);

    Assertions.assertEquals(0, setDecreased.measure());

    // The elements decreased between {} and {1, 2, 3} is {}. The decreased size is 0.
    setDecreased.record(Set.of(1, 2, 3));
    Assertions.assertEquals(0, setDecreased.measure());

    // The elements decreased between {} and {1, 2, 3, 4} is {}. The decreased size is 0.
    setDecreased.record(Set.of(1, 2, 3, 4));
    Assertions.assertEquals(0, setDecreased.measure());

    // The elements decreased between {1, 2, 3} and {1, 2, 3, 4} is {}. The decreased size is 0.
    setDecreased.record(Set.of(1, 2, 3, 4));
    Assertions.assertEquals(0, setDecreased.measure());

    // The elements decreased between {1, 2, 3, 4} and {1} is {2, 3, 4}. The decreased size is 3.
    setDecreased.record(Set.of(1));
    Assertions.assertEquals(3, setDecreased.measure());

    // The elements decreased between {1, 2, 3, 4} and {1, 4, 6} is {2, 3}. The decreased size is 2.
    setDecreased.record(Set.of(1, 4, 6));
    Assertions.assertEquals(2, setDecreased.measure());
  }
}
