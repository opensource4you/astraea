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
package org.astraea.common.balancer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BalancerProblemFormatTest {

  @Test
  void testSerializationFromJson() {
    var payload =
        """
        {
          "timeout": "5s",
          "balancer": "org.astraea.common.balancer.algorithms.GreedyBalancer",
          "balancerConfig": {
            "shuffle.tweaker.min.step": "1",
            "shuffle.tweaker.max.step": "10"
          },
          "clusterCosts": [
            { "cost": "org.astraea.common.cost.ReplicaLeaderCost", "weight": 1 }
          ],
          "moveCosts": [
            "org.astraea.common.cost.ReplicaLeaderCost",
            "org.astraea.common.cost.RecordSizeCost"
          ],
          "costConfig": {
            "max.migrated.size": "500MB",
            "max.migrated.leader.number": 5
          }
        }
        """;
    var timeout = Duration.ofSeconds(5);
    var balancer = "org.astraea.common.balancer.algorithms.GreedyBalancer";
    var balancerConfig =
        Map.ofEntries(
            Map.entry("shuffle.tweaker.min.step", "1"),
            Map.entry("shuffle.tweaker.max.step", "10"));
    var costWeight = List.of(costWeight("org.astraea.common.cost.ReplicaLeaderCost", 1.0));
    var moveCosts =
        Set.of(
            "org.astraea.common.cost.ReplicaLeaderCost", "org.astraea.common.cost.RecordSizeCost");
    var costConfig =
        Map.ofEntries(
            Map.entry("max.migrated.size", "500MB"), Map.entry("max.migrated.leader.number", "5"));

    var converted =
        JsonConverter.defaultConverter().fromJson(payload, TypeRef.of(BalancerProblemFormat.class));

    Assertions.assertEquals(timeout, converted.timeout);
    Assertions.assertEquals(balancer, converted.balancer);
    Assertions.assertEquals(balancerConfig, converted.balancerConfig);
    Assertions.assertEquals(costWeight, converted.clusterCosts);
    Assertions.assertEquals(moveCosts, converted.moveCosts);
    Assertions.assertEquals(costConfig, converted.costConfig);

    var algorithmConfig = converted.parse();
    Assertions.assertEquals(timeout, algorithmConfig.timeout());
    Assertions.assertEquals(balancerConfig, algorithmConfig.balancerConfig().raw());
  }

  @Test
  void testCovertError() {
    {
      var payload =
          """
          { "clusterCosts": [ { "cost": "no.such.cluster.Cost", "weight": 1 } ] }
          """;
      var converted =
          JsonConverter.defaultConverter()
              .fromJson(payload, TypeRef.of(BalancerProblemFormat.class));
      Assertions.assertEquals("no.such.cluster.Cost", converted.clusterCosts.get(0).cost);
      Assertions.assertThrows(IllegalArgumentException.class, converted::parse);
    }
    {
      var payload = """
          { "moveCosts": [ "no.such.move.Cost" ] }
          """;
      var converted =
          JsonConverter.defaultConverter()
              .fromJson(payload, TypeRef.of(BalancerProblemFormat.class));
      Assertions.assertEquals(Set.of("no.such.move.Cost"), converted.moveCosts);
      Assertions.assertThrows(IllegalArgumentException.class, converted::parse);
    }
  }

  private static BalancerProblemFormat.CostWeight costWeight(String cost, double weight) {
    var cw = new BalancerProblemFormat.CostWeight();
    cw.cost = cost;
    cw.weight = weight;
    return cw;
  }
}
