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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.astraea.app.common.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PeriodicTest extends Periodic<Map<Integer, Double>> {
  private double brokerValue = 0.0;

  @Test
  void testTryUpdateAfterOneSecond() {
    var broker1 = tryUpdateAfterOneSecond(this::testMap);
    Assertions.assertEquals(broker1.get(0), 0.0);
    var broker2 = tryUpdateAfterOneSecond(this::testMap);
    Assertions.assertEquals(broker2.get(0), 0.0);
    Utils.sleep(Duration.ofSeconds(1));
    broker2 = tryUpdateAfterOneSecond(this::testMap);
    Assertions.assertEquals(broker2.get(0), 1.0);
  }

  @Test
  void testTryUpdate() {
    var broker1 = tryUpdate(this::testMap, Duration.ofSeconds(3));
    Assertions.assertEquals(broker1.get(0), 0.0);
    var broker2 = tryUpdate(this::testMap, Duration.ofSeconds(3));
    Assertions.assertEquals(broker2.get(0), 0.0);
    Utils.sleep(Duration.ofSeconds(4));
    broker2 = tryUpdate(this::testMap, Duration.ofSeconds(3));
    Assertions.assertEquals(broker2.get(0), 1.0);
  }

  Map<Integer, Double> testMap() {
    var broker = new HashMap<Integer, Double>();
    broker.put(0, brokerValue);
    brokerValue++;
    return broker;
  }
}
