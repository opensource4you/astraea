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

public class SetDifferenceTest {
  @Test
  void testEmpty() {
    var intSetDifference = new SetDifference<Integer>(2);

    Assertions.assertEquals(0, intSetDifference.measure().currentSize());
    Assertions.assertEquals(0, intSetDifference.measure().increasedNum());
    Assertions.assertEquals(0, intSetDifference.measure().removedNum());
    Assertions.assertEquals(0, intSetDifference.measure().unchangedNum());
  }

  @Test
  void testCurrent() {
    var stringSetDifference = new SetDifference<String>(2);

    stringSetDifference.record(
        SetDifference.SetOperation.ofAdd(Set.of("first added", "will be removed")));
    stringSetDifference.record(SetDifference.SetOperation.ofRemove(Set.of("will be removed")));
    stringSetDifference.record(SetDifference.SetOperation.ofAdd(Set.of("second added")));

    Assertions.assertEquals(2, stringSetDifference.measure().currentSize());
  }

  @Test
  void testIncreased() {
    var stringSetDifference = new SetDifference<String>(2);

    stringSetDifference.record(
        SetDifference.SetOperation.ofAdd(Set.of("first added", "will be removed")));
    stringSetDifference.record(SetDifference.SetOperation.ofRemove(Set.of("will be removed")));
    stringSetDifference.record(SetDifference.SetOperation.ofAdd(Set.of("second added")));

    Assertions.assertEquals(1, stringSetDifference.measure().increasedNum());
  }

  @Test
  void testRemoved() {
    var stringSetDifference = new SetDifference<String>(2);

    stringSetDifference.record(
        SetDifference.SetOperation.ofAdd(Set.of("first added", "will be removed")));
    stringSetDifference.record(SetDifference.SetOperation.ofRemove(Set.of("will be removed")));
    stringSetDifference.record(SetDifference.SetOperation.ofAdd(Set.of("second added")));

    Assertions.assertEquals(1, stringSetDifference.measure().removedNum());
  }

  @Test
  void testUnchanged() {
    var stringSetDifference = new SetDifference<String>(2);

    stringSetDifference.record(
        SetDifference.SetOperation.ofAdd(Set.of("first added", "will be removed")));
    stringSetDifference.record(SetDifference.SetOperation.ofRemove(Set.of("will be removed")));
    stringSetDifference.record(SetDifference.SetOperation.ofAdd(Set.of("second added")));

    Assertions.assertEquals(1, stringSetDifference.measure().unchangedNum());
  }
}
