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
package org.astraea.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MapUtilsTest {

  @Test
  void testOf1() {
    var map = MapUtils.of("a", 0);
    Assertions.assertEquals(1, map.size());
    Assertions.assertEquals(0, map.get("a"));
  }

  @Test
  void testOf2() {
    var map = MapUtils.of("a", 0, "b", 1);
    Assertions.assertEquals(2, map.size());
    Assertions.assertEquals(0, map.get("a"));
    Assertions.assertEquals(1, map.get("b"));
  }

  @Test
  void testOf3() {
    var map = MapUtils.of("a", 0, "b", 1, "c", 2);
    Assertions.assertEquals(3, map.size());
    Assertions.assertEquals(0, map.get("a"));
    Assertions.assertEquals(1, map.get("b"));
    Assertions.assertEquals(2, map.get("c"));
  }

  @Test
  void testOf4() {
    var map = MapUtils.of("a", 0, "b", 1, "c", 2, "d", 3);
    Assertions.assertEquals(4, map.size());
    Assertions.assertEquals(0, map.get("a"));
    Assertions.assertEquals(1, map.get("b"));
    Assertions.assertEquals(2, map.get("c"));
    Assertions.assertEquals(3, map.get("d"));
  }

  @Test
  void testOf5() {
    var map = MapUtils.of("a", 0, "b", 1, "c", 2, "d", 3, "e", 4);
    Assertions.assertEquals(5, map.size());
    Assertions.assertEquals(0, map.get("a"));
    Assertions.assertEquals(1, map.get("b"));
    Assertions.assertEquals(2, map.get("c"));
    Assertions.assertEquals(3, map.get("d"));
    Assertions.assertEquals(4, map.get("e"));
  }

  @Test
  void testOf6() {
    var map = MapUtils.of("a", 0, "b", 1, "c", 2, "d", 3, "e", 4, "f", 5);
    Assertions.assertEquals(6, map.size());
    Assertions.assertEquals(0, map.get("a"));
    Assertions.assertEquals(1, map.get("b"));
    Assertions.assertEquals(2, map.get("c"));
    Assertions.assertEquals(3, map.get("d"));
    Assertions.assertEquals(4, map.get("e"));
    Assertions.assertEquals(5, map.get("f"));
  }

  @Test
  void testOf7() {
    var map = MapUtils.of("a", 0, "b", 1, "c", 2, "d", 3, "e", 4, "f", 5, "g", 6);
    Assertions.assertEquals(7, map.size());
    Assertions.assertEquals(0, map.get("a"));
    Assertions.assertEquals(1, map.get("b"));
    Assertions.assertEquals(2, map.get("c"));
    Assertions.assertEquals(3, map.get("d"));
    Assertions.assertEquals(4, map.get("e"));
    Assertions.assertEquals(5, map.get("f"));
    Assertions.assertEquals(6, map.get("g"));
  }

  @Test
  void testOf8() {
    var map = MapUtils.of("a", 0, "b", 1, "c", 2, "d", 3, "e", 4, "f", 5, "g", 6, "h", 7);
    Assertions.assertEquals(8, map.size());
    Assertions.assertEquals(0, map.get("a"));
    Assertions.assertEquals(1, map.get("b"));
    Assertions.assertEquals(2, map.get("c"));
    Assertions.assertEquals(3, map.get("d"));
    Assertions.assertEquals(4, map.get("e"));
    Assertions.assertEquals(5, map.get("f"));
    Assertions.assertEquals(6, map.get("g"));
    Assertions.assertEquals(7, map.get("h"));
  }

  @Test
  void testOf9() {
    var map = MapUtils.of("a", 0, "b", 1, "c", 2, "d", 3, "e", 4, "f", 5, "g", 6, "h", 7, "i", 8);
    Assertions.assertEquals(9, map.size());
    Assertions.assertEquals(0, map.get("a"));
    Assertions.assertEquals(1, map.get("b"));
    Assertions.assertEquals(2, map.get("c"));
    Assertions.assertEquals(3, map.get("d"));
    Assertions.assertEquals(4, map.get("e"));
    Assertions.assertEquals(5, map.get("f"));
    Assertions.assertEquals(6, map.get("g"));
    Assertions.assertEquals(7, map.get("h"));
    Assertions.assertEquals(8, map.get("i"));
  }

  @Test
  void testOf10() {
    var map =
        MapUtils.of("a", 0, "b", 1, "c", 2, "d", 3, "e", 4, "f", 5, "g", 6, "h", 7, "i", 8, "j", 9);
    Assertions.assertEquals(10, map.size());
    Assertions.assertEquals(0, map.get("a"));
    Assertions.assertEquals(1, map.get("b"));
    Assertions.assertEquals(2, map.get("c"));
    Assertions.assertEquals(3, map.get("d"));
    Assertions.assertEquals(4, map.get("e"));
    Assertions.assertEquals(5, map.get("f"));
    Assertions.assertEquals(6, map.get("g"));
    Assertions.assertEquals(7, map.get("h"));
    Assertions.assertEquals(8, map.get("i"));
    Assertions.assertEquals(9, map.get("j"));
  }
}
