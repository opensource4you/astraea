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

    Assertions.assertEquals(Set.of(), intSetDifference.measure().current());
    Assertions.assertEquals(Set.of(), intSetDifference.measure().increased());
    Assertions.assertEquals(Set.of(), intSetDifference.measure().removed());
    Assertions.assertEquals(Set.of(), intSetDifference.measure().unchanged());
  }

  @Test
  void testCurrent() {
    var stringSetDifference = new SetDifference<String>(2);

    stringSetDifference.record(
        SetDifference.SetOperation.ofAdd(Set.of("first added", "will be removed")));
    stringSetDifference.record(SetDifference.SetOperation.ofRemove(Set.of("will be removed")));
    stringSetDifference.record(SetDifference.SetOperation.ofAdd(Set.of("second added")));

    Assertions.assertEquals(
        Set.of("first added", "second added"), stringSetDifference.measure().current());
  }

  @Test
  void testIncreased() {
    var stringSetDifference = new SetDifference<String>(2);

    stringSetDifference.record(
        SetDifference.SetOperation.ofAdd(Set.of("first added", "will be removed")));
    stringSetDifference.record(SetDifference.SetOperation.ofRemove(Set.of("will be removed")));
    stringSetDifference.record(SetDifference.SetOperation.ofAdd(Set.of("second added")));

    Assertions.assertEquals(Set.of("second added"), stringSetDifference.measure().increased());
  }

  @Test
  void testRemoved() {
    var stringSetDifference = new SetDifference<String>(2);

    stringSetDifference.record(
        SetDifference.SetOperation.ofAdd(Set.of("first added", "will be removed")));
    stringSetDifference.record(SetDifference.SetOperation.ofRemove(Set.of("will be removed")));
    stringSetDifference.record(SetDifference.SetOperation.ofAdd(Set.of("second added")));

    Assertions.assertEquals(Set.of("will be removed"), stringSetDifference.measure().removed());
  }

  @Test
  void testUnchanged() {
    var stringSetDifference = new SetDifference<String>(2);

    stringSetDifference.record(
        SetDifference.SetOperation.ofAdd(Set.of("first added", "will be removed")));
    stringSetDifference.record(SetDifference.SetOperation.ofRemove(Set.of("will be removed")));
    stringSetDifference.record(SetDifference.SetOperation.ofAdd(Set.of("second added")));

    Assertions.assertEquals(Set.of("first added"), stringSetDifference.measure().unchanged());
  }
}
