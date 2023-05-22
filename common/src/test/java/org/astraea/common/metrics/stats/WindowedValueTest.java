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

import java.time.Duration;
import java.util.List;
import org.astraea.common.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WindowedValueTest {
  @Test
  void testGet() {
    var windowedValue = new WindowedValue<Integer>(Duration.ofMillis(20));

    windowedValue.add(1);
    Utils.sleep(Duration.ofMillis(10));
    windowedValue.add(2);

    // 1 is created 10 millisecond before.
    // 2 is created just now.
    // So, 1 and 2 should be in the window of 20 millisecond.
    Assertions.assertEquals(List.of(1, 2), windowedValue.get());

    Utils.sleep(Duration.ofMillis(10));
    // Now,
    // 1 is created 20 millisecond before.
    // 2 is created 10 millisecond before.
    // 1 should not be in the window, so the returned list contains only 2
    Assertions.assertEquals(List.of(2), windowedValue.get());
  }
}
