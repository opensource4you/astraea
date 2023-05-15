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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConfigurationTest {

  @Test
  void testString() {
    var config = Configuration.of(Map.of("key", "value"));
    Assertions.assertEquals(Optional.of("value"), config.string("key"));
    Assertions.assertEquals("value", config.requireString("key"));
  }

  @Test
  void testList() {
    var config = Configuration.of(Map.of("key", "v0,v1"));
    Assertions.assertEquals(List.of("v0", "v1"), config.list("key", ","));
  }

  @Test
  void testFilteredConfigs() {
    var config = Configuration.of(Map.of("key", "v1", "filtered.key", "v2", "key.filtered", "v3"));
    Assertions.assertEquals(Map.of("key", "v2"), config.filteredPrefixConfigs("filtered").raw());
  }

  @Test
  void testDuration() {
    var config = Configuration.of(Map.of("wait.time", "15ms", "response", "3s"));
    var waitTime = config.duration("wait.time");
    var response = config.duration("response");
    var empty = config.duration("walala");
    Assertions.assertEquals(Utils.toDuration("15ms"), waitTime.orElseThrow());
    Assertions.assertEquals(Utils.toDuration("3s"), response.orElseThrow());
    Assertions.assertTrue(empty.isEmpty());
  }

  @Test
  void testLong() {
    var config = Configuration.of(Map.of("long.value", "2147483648"));
    Assertions.assertEquals(2147483648L, config.longInteger("long.value").orElse(0L));
  }

  @Test
  void testDataSize() {
    var config = Configuration.of(Map.of("upper.bound", "30MiB", "traffic.interval", "5MB"));
    var upper = config.dataSize("upper.bound");
    var interval = config.dataSize("traffic.interval");
    var empty = config.dataSize("kekw");

    Assertions.assertEquals(DataRate.MiB.of(30).dataSize().bytes(), upper.get().bytes());
    Assertions.assertEquals(DataRate.MB.of(5).dataSize().bytes(), interval.get().bytes());
    Assertions.assertTrue(empty.isEmpty());
  }
}
