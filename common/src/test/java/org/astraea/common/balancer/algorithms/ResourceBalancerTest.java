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
package org.astraea.common.balancer.algorithms;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.cost.ResourceCapacity;
import org.astraea.common.cost.ResourceUsage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ResourceBalancerTest {

  @Test
  void testUsageDominationComparator() {
    var replica =
        Replica.builder()
            .topic("topic")
            .partition(0)
            .nodeInfo(NodeInfo.of(0, "", -1))
            .size(0)
            .path("")
            .buildLeader();
    var replicaA = Replica.builder(replica).build();
    var replicaB = Replica.builder(replica).build();
    var replicaC = Replica.builder(replica).build();
    var replicaD = Replica.builder(replica).build();
    var replicaE = Replica.builder(replica).build();
    var resourceA = new ResourceUsage(Map.of("network", 100.0, "storage", 100.0));
    var resourceB = new ResourceUsage(Map.of("network", 100.0, "storage", 50.0));
    var resourceC = new ResourceUsage(Map.of("network", 50.0, "storage", 100.0));
    var resourceD = new ResourceUsage(Map.of("network", 50.0, "storage", 50.0));
    var resourceE = new ResourceUsage(Map.of("network", 50.0));
    Function<Replica, ResourceUsage> usageFunction =
        (r) -> {
          if (r == replicaA) return resourceA;
          if (r == replicaB) return resourceB;
          if (r == replicaC) return resourceC;
          if (r == replicaD) return resourceD;
          if (r == replicaE) return resourceE;
          throw new IllegalStateException();
        };
    var comparator = ResourceBalancer.AlgorithmContext.usageDominationComparator(usageFunction);

    var AssertionsHelper =
        new Object() {
          void assertOrder(Replica... replicas) {
            for (int i = 0; i < replicas.length; i++) {
              for (int j = i + 1; j < replicas.length; j++) {
                Assertions.assertTrue(
                    comparator.compare(replicas[i], replicas[j]) < 0,
                    String.format(
                        "Replica[%d] < Replica[%d] is not correct. (Usage of lhs %s) (Usage of rhs %s)",
                        i, j, usageFunction.apply(replicas[i]), usageFunction.apply(replicas[j])));
              }
            }
          }

          void assertEqualOrder(Replica a, Replica b) {
            Assertions.assertEquals(
                0,
                comparator.compare(a, b),
                String.format("%s %s", usageFunction.apply(a), usageFunction.apply(b)));
          }
        };

    // Order by resource usage A > B > D > E
    AssertionsHelper.assertOrder(replicaA, replicaB, replicaD, replicaE);
    // Order by resource usage A > C > D > E
    AssertionsHelper.assertOrder(replicaA, replicaC, replicaD, replicaE);
    // Resource usage of B and C can be considered as equal
    AssertionsHelper.assertEqualOrder(replicaB, replicaC);
    // even though D and E has same network usage. E lack of storage usage so is considered as less
    // resource used.
    AssertionsHelper.assertOrder(replicaA, replicaB, replicaD, replicaE);
  }

  @Test
  void testIdealnessDominationComparator() {
    class AbsoluteResourceCapacity implements ResourceCapacity {

      private final String name;
      private final double optimalUse;

      AbsoluteResourceCapacity(String name, double optimalUse) {
        this.name = name;
        this.optimalUse = optimalUse;
      }

      @Override
      public String resourceName() {
        return name;
      }

      @Override
      public double optimalUsage() {
        return optimalUse;
      }

      @Override
      public Comparator<ResourceUsage> usageIdealnessComparator() {
        return Comparator.comparingDouble(
            usage -> Math.abs(usage.usage().getOrDefault(resourceName(), 0.0) - optimalUsage()));
      }

      @Override
      public Predicate<ResourceUsage> usageValidnessPredicate() {
        return null;
      }
    }

    var capacityIngress = new AbsoluteResourceCapacity("ingress", 50.0);
    var capacityEgress = new AbsoluteResourceCapacity("egress", 100.0);
    var comparator =
        ResourceBalancer.AlgorithmContext.usageIdealnessDominationComparator(
            List.of(capacityIngress, capacityEgress));
    var usageA = new ResourceUsage(Map.of("ingress", 50.0, "egress", 100.0));
    var usageB = new ResourceUsage(Map.of("ingress", 25.0, "egress", 100.0));
    var usageC = new ResourceUsage(Map.of("ingress", 50.0, "egress", 50.0));
    var usageD = new ResourceUsage(Map.of("ingress", 25.0, "egress", 50.0));
    var usageE = new ResourceUsage(Map.of("ingress", 0.0, "egress", 0.0));
    var usageF = new ResourceUsage(Map.of("ingress", 0.0));

    var AssertionsHelper =
        new Object() {
          void assertOrder(ResourceUsage... usages) {
            for (int i = 0; i < usages.length; i++) {
              for (int j = i + 1; j < usages.length; j++) {
                Assertions.assertTrue(
                    comparator.compare(usages[i], usages[j]) < 0,
                    String.format(
                        "ResourceUsage[%d] < ResourceUsage[%d] is not correct. (Usage of lhs %s) (Usage of rhs %s)",
                        i, j, usages[i], usages[j]));
              }
            }
          }

          void assertEqualOrder(ResourceUsage a, ResourceUsage b) {
            Assertions.assertEquals(0, comparator.compare(a, b), String.format("%s %s", a, b));
          }
        };

    // usage idealness A > B = C > D
    AssertionsHelper.assertOrder(usageA, usageB, usageD);
    AssertionsHelper.assertOrder(usageA, usageC, usageD);
    // both B and C didn't dominate each other
    AssertionsHelper.assertEqualOrder(usageB, usageC);
    // lack of resource field is considered as zero
    AssertionsHelper.assertEqualOrder(usageE, usageF);
  }
}
