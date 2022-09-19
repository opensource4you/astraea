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
package org.astraea.common.partitioner;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.astraea.common.cost.Configuration;
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
  void testMap() {
    var config = Configuration.of(Map.of("key", "v0:0,v1:1"));
    Assertions.assertEquals(
        Map.of("v0", 0, "v1", 1), config.map("key", ",", ":", Integer::valueOf));
  }
}
