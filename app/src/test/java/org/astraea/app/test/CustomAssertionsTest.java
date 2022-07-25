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
package org.astraea.app.test;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

public class CustomAssertionsTest {

  @Test
  void testAssertContain() {
    CustomAssertions.assertContain(List.of("t1", "t2"), List.of("t1", "t2", "t3"));
    CustomAssertions.assertContain(Set.of("t1", "t2"), Set.of("t1", "t2", "t3"));

    Assertions.assertThrowsExactly(
        AssertionFailedError.class,
        () -> CustomAssertions.assertContain(List.of("t1", "t2", "t4"), List.of("t1", "t2", "t3")));
    Assertions.assertThrowsExactly(
        AssertionFailedError.class,
        () -> CustomAssertions.assertContain(Set.of("t1", "t2", "t4"), Set.of("t1", "t2", "t3")));
  }
}
