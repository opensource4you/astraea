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
package org.astraea.app.balancer.log;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class LogPlacementTest {

  @Test
  @DisplayName("Equals")
  void isMatch0() {
    final var placement0 =
        List.of(LogPlacement.of(0, "/A"), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    final var placement1 =
        List.of(LogPlacement.of(0, "/A"), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    Assertions.assertTrue(LogPlacement.isMatch(placement0, placement1));
  }

  @Test
  @DisplayName("Wrong order")
  void noMatch0() {
    final var placement0 =
        List.of(LogPlacement.of(0, "/A"), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    final var placement1 =
        List.of(LogPlacement.of(1, "/B"), LogPlacement.of(0, "/A"), LogPlacement.of(2, "/C"));
    Assertions.assertFalse(LogPlacement.isMatch(placement0, placement1));
  }

  @Test
  @DisplayName("No log dir name match")
  void noMatch1() {
    final var placement0 =
        List.of(LogPlacement.of(0, "/A"), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    final var placement1 =
        List.of(LogPlacement.of(0, "/Aaa"), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    Assertions.assertFalse(LogPlacement.isMatch(placement0, placement1));
  }

  @Test
  @DisplayName("Optional log dir")
  void noMatch2() {
    final var sourcePlacement =
        List.of(LogPlacement.of(0, "/A"), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    final var targetPlacement =
        List.of(LogPlacement.of(0), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    // don't care which log dir placement[0] will eventually be.
    Assertions.assertTrue(LogPlacement.isMatch(sourcePlacement, targetPlacement));
  }

  @Test
  @DisplayName("Optional log dir")
  void noMatch3() {
    final var sourcePlacement =
        List.of(LogPlacement.of(0), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    final var targetPlacement =
        List.of(LogPlacement.of(0, "/A"), LogPlacement.of(1, "/B"), LogPlacement.of(2, "/C"));
    // do care which log dir placement[0] will eventually be.
    Assertions.assertFalse(LogPlacement.isMatch(sourcePlacement, targetPlacement));
  }
}
