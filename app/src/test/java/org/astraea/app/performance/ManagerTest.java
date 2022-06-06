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
package org.astraea.app.performance;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ManagerTest {

  @Test
  void testGetKey() {
    var argument = new Performance.Argument();

    argument.keyDistributionType = DistributionType.UNIFORM;
    var manager = new Manager(argument, List.of(), List.of());
    Assertions.assertTrue(manager.getKey().length > 0);

    argument.keyDistributionType = DistributionType.ZIPFIAN;
    manager = new Manager(argument, List.of(), List.of());
    Assertions.assertTrue(manager.getKey().length > 0);

    argument.keyDistributionType = DistributionType.LATEST;
    manager = new Manager(argument, List.of(), List.of());
    Assertions.assertTrue(manager.getKey().length > 0);

    argument.keyDistributionType = DistributionType.FIXED;
    manager = new Manager(argument, List.of(), List.of());
    Assertions.assertTrue(manager.getKey().length > 0);
  }

  @Test
  void testConsumerDone() {
    var argument = new Performance.Argument();
    var producerMetrics = new Metrics();
    var consumerMetrics = new Metrics();

    argument.exeTime = ExeTime.of("1records");
    var manager = new Manager(argument, List.of(producerMetrics), List.of(consumerMetrics));

    // Produce one record
    producerMetrics.accept(0L, 0L);
    Assertions.assertFalse(manager.consumedDone());

    // Consume one record
    consumerMetrics.accept(0L, 0L);
    Assertions.assertTrue(manager.consumedDone());

    // Test zero consumer. (run for one record)
    manager = new Manager(argument, List.of(producerMetrics), List.of());
    Assertions.assertTrue(manager.consumedDone());
  }
}
