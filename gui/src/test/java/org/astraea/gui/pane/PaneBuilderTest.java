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
package org.astraea.gui.pane;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PaneBuilderTest {

  @Test
  void testWildcard() {
    var pattern0 = PaneBuilder.wildcardToPattern("aaa*");
    Assertions.assertTrue(pattern0.matcher("aaa").matches());
    Assertions.assertTrue(pattern0.matcher("aaab").matches());
    Assertions.assertTrue(pattern0.matcher("aaacc").matches());
    Assertions.assertFalse(pattern0.matcher("bbaaa").matches());
    Assertions.assertFalse(pattern0.matcher("ccaaadd").matches());
    Assertions.assertFalse(pattern0.matcher("aa").matches());

    var pattern1 = PaneBuilder.wildcardToPattern("*aaa*");
    Assertions.assertTrue(pattern1.matcher("aaa").matches());
    Assertions.assertTrue(pattern1.matcher("aaab").matches());
    Assertions.assertTrue(pattern1.matcher("aaacc").matches());
    Assertions.assertTrue(pattern1.matcher("bbaaa").matches());
    Assertions.assertTrue(pattern1.matcher("ccaaadd").matches());
    Assertions.assertFalse(pattern1.matcher("aa").matches());

    var pattern2 = PaneBuilder.wildcardToPattern("?aaa*");
    Assertions.assertFalse(pattern2.matcher("aaa").matches());
    Assertions.assertFalse(pattern2.matcher("aaab").matches());
    Assertions.assertFalse(pattern2.matcher("aaacc").matches());
    Assertions.assertTrue(pattern2.matcher("baaa").matches());
    Assertions.assertTrue(pattern2.matcher("caaadd").matches());
    Assertions.assertFalse(pattern2.matcher("aa").matches());

    var pattern3 = PaneBuilder.wildcardToPattern("192*");
    Assertions.assertTrue(pattern3.matcher("192.168").matches());
  }
}
