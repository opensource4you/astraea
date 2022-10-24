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
package org.astraea.common.scenario;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.astraea.common.Utils;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class SkewedPartitionScenarioTest extends RequireBrokerCluster {

  @CsvSource(
      value = {
        // partitions, replicas
        "           1,        1",
        "           1,        2",
        "           1,        3",
        "           2,        1",
        "           2,        2",
        "           3,        2",
        "           3,        3",
        "           5,        3",
        "          10,        3",
        "         100,        1",
        "         100,        2",
        "         100,        3",
      })
  @ParameterizedTest
  void test(int partitions, short replicas) throws ExecutionException, InterruptedException {
    var topicName = Utils.randomString();
    var scenario = new SkewedPartitionScenario(topicName, partitions, replicas, 0.5);
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      var result = scenario.apply(admin).toCompletableFuture().get();
      Assertions.assertEquals(topicName, result.topicName());
      Assertions.assertEquals(partitions, result.numberOfPartitions());
      Assertions.assertEquals(replicas, result.numberOfReplicas());
      Assertions.assertEquals(
          partitions, result.leaderSum().values().stream().mapToLong(x -> x).sum());
      Assertions.assertEquals(
          (long) partitions * replicas, result.logSum().values().stream().mapToLong(x -> x).sum());
    }
  }

  @ValueSource(ints = {0, 1, 2, 3, 4, 5})
  @ParameterizedTest
  void testSampledReplicaList(int selection) {
    for (int i = 0; i < 100; i++) {
      var brokerIds = List.of(0, 1, 2, 3, 4, 5);
      var distribution = new BinomialDistribution(brokerIds.size() - 1, 0.5);
      var selections =
          SkewedPartitionScenario.sampledReplicaList(brokerIds, selection, distribution);
      Assertions.assertTrue(brokerIds.containsAll(selections), "Valid candidates");
      Assertions.assertEquals(selection, Set.copyOf(selections).size(), "No duplicate items");
    }
  }
}
