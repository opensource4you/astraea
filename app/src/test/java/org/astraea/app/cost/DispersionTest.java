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

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DispersionTest {

  @Test
  void testCorrelationCoefficient() {
    var dispersion = Dispersion.correlationCoefficient();
    var scores = List.of(0.2, 0.4, 0.7);
    Assertions.assertEquals(0.47418569253607507, dispersion.calculate(scores));

    var zeroScores = List.of(0.0, 0.0, 0.0);
    var score = dispersion.calculate(zeroScores);
    Assertions.assertFalse(Double.isNaN(score));
    Assertions.assertEquals(0.0, score);
  }
}
