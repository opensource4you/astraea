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
package org.astraea.connector.perf;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.connector.ConnectorClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PerfSourceTaskTest {

  @Test
  void testInit() {
    var task = new PerfSourceTask();
    task.init(Configuration.of(Map.of(ConnectorClient.TOPICS_KEY, "a")));
    Assertions.assertNotNull(task.rand);
    Assertions.assertNotNull(task.topics);
    Assertions.assertNotNull(task.frequency);
    Assertions.assertNotNull(task.keySelector);
    Assertions.assertNotNull(task.keySizeGenerator);
    Assertions.assertNotNull(task.keys);
    Assertions.assertNotNull(task.valueSelector);
    Assertions.assertNotNull(task.valueSizeGenerator);
    Assertions.assertNotNull(task.values);
  }

  @Test
  void testKeyAndValue() {
    var task = new PerfSourceTask();
    task.init(
        Configuration.of(
            Map.of(
                ConnectorClient.TOPICS_KEY,
                "a",
                PerfSource.KEY_DISTRIBUTION_DEF.name(),
                "uniform",
                PerfSource.VALUE_DISTRIBUTION_DEF.name(),
                "uniform")));
    var keys =
        IntStream.range(0, 100)
            .mapToObj(ignored -> Optional.ofNullable(task.key()))
            .flatMap(Optional::stream)
            .collect(Collectors.toSet());
    Assertions.assertEquals(keys.size(), task.keys.size());
    Assertions.assertNotEquals(0, keys.size());
    var values =
        IntStream.range(0, 100)
            .mapToObj(ignored -> Optional.ofNullable(task.value()))
            .flatMap(Optional::stream)
            .collect(Collectors.toSet());
    Assertions.assertEquals(values.size(), task.values.size());
    Assertions.assertNotEquals(0, values.size());
  }

  @Test
  void testZeroKeySize() {
    var task = new PerfSourceTask();
    task.init(
        Configuration.of(
            Map.of(ConnectorClient.TOPICS_KEY, "a", PerfSource.KEY_LENGTH_DEF.name(), "0byte")));
    Assertions.assertNull(task.key());
    Assertions.assertEquals(0, task.keys.size());
  }

  @Test
  void testZeroValueSize() {
    var task = new PerfSourceTask();
    task.init(
        Configuration.of(
            Map.of(ConnectorClient.TOPICS_KEY, "a", PerfSource.VALUE_LENGTH_DEF.name(), "0byte")));
    Assertions.assertNull(task.value());
    Assertions.assertEquals(0, task.values.size());
  }
}
