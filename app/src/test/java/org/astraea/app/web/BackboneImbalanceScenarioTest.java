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
package org.astraea.app.web;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.astraea.common.Configuration;
import org.astraea.common.admin.Admin;
import org.astraea.it.Service;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BackboneImbalanceScenarioTest {

  @Test
  void testApply() {
    try (var service = Service.builder().numberOfBrokers(3).build()) {
      try (Admin admin = Admin.of(service.bootstrapServers())) {
        var scenario = new BackboneImbalanceScenario();
        var result =
            scenario
                .apply(
                    admin,
                    Configuration.of(
                        Map.ofEntries(
                            Map.entry(BackboneImbalanceScenario.CONFIG_TOPIC_COUNT, "100"))))
                .toCompletableFuture()
                .join();

        Assertions.assertEquals(101, result.totalTopics());
      }
    }
  }

  @Test
  void testSeedWorks() {
    var scenario = new BackboneImbalanceScenario();
    var seed = ThreadLocalRandom.current().nextInt();
    var config =
        Configuration.of(
            Map.ofEntries(
                Map.entry(BackboneImbalanceScenario.CONFIG_RANDOM_SEED, Integer.toString(seed)),
                Map.entry(BackboneImbalanceScenario.CONFIG_TOPIC_COUNT, "50")));
    try (var service0 = Service.builder().numberOfBrokers(3).build();
        var service1 = Service.builder().numberOfBrokers(3).build(); ) {
      try (Admin admin0 = Admin.of(service0.bootstrapServers());
          Admin admin1 = Admin.of(service1.bootstrapServers())) {
        var result0 = scenario.apply(admin0, config).toCompletableFuture().join();
        var result1 = scenario.apply(admin1, config).toCompletableFuture().join();

        Assertions.assertEquals(result0.topicDataRate(), result1.topicDataRate());
        Assertions.assertEquals(result0.topicDataRateHistogram(), result1.topicDataRateHistogram());
      }
    }
  }
}
