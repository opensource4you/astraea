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

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DispersionTest {

  @Test
  void testStandardDeviation() {
    var dispersion = Dispersion.standardDeviation();
    var scores = List.of(8, 8, 4, 4);
    Assertions.assertEquals(2, dispersion.calculate(scores));

    var zeroScores = List.of(0.0, 0.0, 0.0);
    var score = dispersion.calculate(zeroScores);
    Assertions.assertFalse(Double.isNaN(score));
    Assertions.assertEquals(0.0, score);
  }

  @Test
  void testNormalizedStandardDeviation() {
    // test calculate
    var scores = List.of(8, 8, 4, 4);
    var normalizedSD = Dispersion.normalizedStandardDeviation();
    var standardDeviation = Dispersion.standardDeviation();
    var total = scores.stream().mapToDouble(x -> x).sum();
    var sd = scores.stream().map(x -> x / total).toList();
    Assertions.assertEquals(standardDeviation.calculate(sd), normalizedSD.calculate(scores));

    // test zero
    var zeroScores = List.of(0.0, 0.0, 0.0);
    var score = normalizedSD.calculate(zeroScores);
    Assertions.assertFalse(Double.isNaN(score));
    Assertions.assertEquals(0.0, score);

    // the test standard deviation interval is between [0,1]
    var dispersion = Dispersion.normalizedStandardDeviation();
    var list1 = List.of(0, Integer.MAX_VALUE);
    var list2 = List.of(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, Integer.MAX_VALUE);
    var list3 = List.of(-1 * Integer.MAX_VALUE, Integer.MAX_VALUE);
    var cost1 = dispersion.calculate(list1);
    var cost2 = dispersion.calculate(list2);
    var cost3 = dispersion.calculate(list3);
    Assertions.assertTrue(cost1 <= 1 && cost1 >= 0);
    Assertions.assertTrue(cost2 <= 1 && cost2 >= 0);
    Assertions.assertTrue(cost3 <= 1 && cost3 >= 0);
  }
}
