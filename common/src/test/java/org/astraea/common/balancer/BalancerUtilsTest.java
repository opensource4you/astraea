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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BalancerUtilsTest {

  @Test
  void testBalancingMode() {
    var cluster = ClusterInfo.builder().addNode(Set.of(1, 2, 3, 4, 5)).build();

    Assertions.assertThrows(Exception.class, () -> BalancerUtils.balancingMode(cluster, "bad"));
    Assertions.assertThrows(Exception.class, () -> BalancerUtils.balancingMode(cluster, "bad:bad"));
    Assertions.assertThrows(
        Exception.class, () -> BalancerUtils.balancingMode(cluster, "bad:bad:bad"));
    Assertions.assertThrows(
        Exception.class, () -> BalancerUtils.balancingMode(cluster, "1:balancing,bad:bad"));
    Assertions.assertThrows(
        Exception.class, () -> BalancerUtils.balancingMode(cluster, "1:balancing,bad:bad:bad"));
    Assertions.assertThrows(
        Exception.class,
        () -> BalancerUtils.balancingMode(cluster, "1:balancing,2:demoted,3:excluded,4:oops"));
    Assertions.assertThrows(
        Exception.class,
        () -> BalancerUtils.balancingMode(cluster, "1:balancing,2:demoted,3:excluded,4:"));
    Assertions.assertThrows(
        Exception.class,
        () -> BalancerUtils.balancingMode(cluster, "1:balancing,2:demoted,3:excluded,1:"));
    Assertions.assertThrows(
        Exception.class,
        () -> BalancerUtils.balancingMode(cluster, "1:balancing,2:demoted,3:excluded,:"));
    Assertions.assertThrows(
        Exception.class,
        () -> BalancerUtils.balancingMode(cluster, "1:balancing,2:demoted,3:excluded,::"));
    Assertions.assertThrows(Exception.class, () -> BalancerUtils.balancingMode(cluster, "1:"));
    Assertions.assertThrows(
        Exception.class, () -> BalancerUtils.balancingMode(cluster, "1:balancing,1:balancing"));

    Assertions.assertDoesNotThrow(
        () -> BalancerUtils.balancingMode(cluster, "reserved_usage:balancing"),
        "Intentionally reserved this usage");

    Assertions.assertEquals(
        BalancerUtils.BalancingModes.BALANCING,
        BalancerUtils.balancingMode(cluster, "").get(1),
        "default");
    Assertions.assertEquals(
        BalancerUtils.BalancingModes.DEMOTED,
        BalancerUtils.balancingMode(cluster, "1:demoted").get(1),
        "value");
    Assertions.assertEquals(
        BalancerUtils.BalancingModes.DEMOTED,
        BalancerUtils.balancingMode(cluster, "default:demoted").get(5),
        "user defined default");
    Assertions.assertEquals(
        BalancerUtils.BalancingModes.EXCLUDED,
        BalancerUtils.balancingMode(cluster, "3:excluded,4:excluded").get(3));
    Assertions.assertEquals(
        BalancerUtils.BalancingModes.EXCLUDED,
        BalancerUtils.balancingMode(cluster, "3:excluded,4:excluded").get(4));
    Assertions.assertEquals(
        BalancerUtils.BalancingModes.BALANCING,
        BalancerUtils.balancingMode(cluster, "3:excluded,4:excluded,1:balancing").get(1));
    Assertions.assertEquals(
        Set.of(1, 2, 3, 4, 5), BalancerUtils.balancingMode(cluster, "").keySet());
  }

  @Test
  void testVerifyClearBrokerValidness() {
    var base =
        ClusterInfo.builder()
            .addNode(Set.of(1, 2, 3))
            .addFolders(
                Map.ofEntries(
                    Map.entry(1, Set.of("/folder")),
                    Map.entry(2, Set.of("/folder")),
                    Map.entry(3, Set.of("/folder"))))
            .build();
    var iter = Stream.of(1, 2, 3).map(base::node).iterator();
    var cluster =
        ClusterInfo.builder(base)
            .addTopic("A", 1, (short) 1, r -> Replica.builder(r).broker(iter.next()).build())
            .addTopic("B", 1, (short) 1, r -> Replica.builder(r).broker(iter.next()).build())
            .addTopic("C", 1, (short) 1, r -> Replica.builder(r).broker(iter.next()).build())
            .build();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            BalancerUtils.verifyClearBrokerValidness(cluster, id -> id == 1, t -> !t.equals("A")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            BalancerUtils.verifyClearBrokerValidness(cluster, id -> id == 2, t -> !t.equals("B")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            BalancerUtils.verifyClearBrokerValidness(cluster, id -> id == 3, t -> !t.equals("C")));
    Assertions.assertDoesNotThrow(
        () -> BalancerUtils.verifyClearBrokerValidness(cluster, id -> id == 1, t -> t.equals("A")));
    Assertions.assertDoesNotThrow(
        () -> BalancerUtils.verifyClearBrokerValidness(cluster, id -> id == 2, t -> t.equals("B")));
    Assertions.assertDoesNotThrow(
        () -> BalancerUtils.verifyClearBrokerValidness(cluster, id -> id == 3, t -> t.equals("C")));

    var hasAdding =
        ClusterInfo.builder(cluster).mapLog(r -> Replica.builder(r).isAdding(true).build()).build();
    var hasRemoving =
        ClusterInfo.builder(cluster)
            .mapLog(r -> Replica.builder(r).isRemoving(true).build())
            .build();
    var hasFuture =
        ClusterInfo.builder(cluster).mapLog(r -> Replica.builder(r).isFuture(true).build()).build();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> BalancerUtils.verifyClearBrokerValidness(hasAdding, x -> true, x -> true));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> BalancerUtils.verifyClearBrokerValidness(hasRemoving, x -> true, x -> true));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> BalancerUtils.verifyClearBrokerValidness(hasFuture, x -> true, x -> true));
    Assertions.assertDoesNotThrow(
        () -> BalancerUtils.verifyClearBrokerValidness(hasAdding, x -> false, x -> true));
    Assertions.assertDoesNotThrow(
        () -> BalancerUtils.verifyClearBrokerValidness(hasRemoving, x -> false, x -> true));
    Assertions.assertDoesNotThrow(
        () -> BalancerUtils.verifyClearBrokerValidness(hasFuture, x -> false, x -> true));
  }

  @Test
  void testClearedCluster() {
    var cluster =
        ClusterInfo.builder()
            .addNode(Set.of(1, 2))
            .addFolders(Map.of(1, Set.of("/folder")))
            .addFolders(Map.of(2, Set.of("/folder")))
            .addTopic("topic", 100, (short) 2)
            .addNode(Set.of(3, 4))
            .addFolders(Map.of(3, Set.of("/folder")))
            .addFolders(Map.of(4, Set.of("/folder")))
            .build();
    Assertions.assertThrows(
        Exception.class,
        () -> BalancerUtils.clearedCluster(cluster, id -> id == 1 || id == 2, id -> id == 3),
        "Insufficient brokers to meet replica factor requirement");
    var clearedCluster =
        Assertions.assertDoesNotThrow(
            () ->
                BalancerUtils.clearedCluster(
                    cluster, id -> id == 1 || id == 2, id -> id == 3 || id == 4));

    Assertions.assertEquals(
        List.of(), clearedCluster.replicas().stream().filter(x -> x.broker().id() == 1).toList());
    Assertions.assertEquals(
        List.of(), clearedCluster.replicas().stream().filter(x -> x.broker().id() == 2).toList());
    Assertions.assertNotEquals(
        List.of(), clearedCluster.replicas().stream().filter(x -> x.broker().id() == 3).toList());
    Assertions.assertNotEquals(
        List.of(), clearedCluster.replicas().stream().filter(x -> x.broker().id() == 4).toList());

    var sameCluster =
        Assertions.assertDoesNotThrow(
            () -> BalancerUtils.clearedCluster(cluster, id -> false, id -> true));
    Assertions.assertEquals(
        Set.of(),
        ClusterInfo.findNonFulfilledAllocation(cluster, sameCluster),
        "Nothing to clear, nothing to change");

    var aCluster =
        Assertions.assertDoesNotThrow(
            () -> BalancerUtils.clearedCluster(cluster, id -> id == 1, id -> id == 3));
    Assertions.assertEquals(
        0, aCluster.replicas().stream().filter(r -> r.broker().id() == 1).count(), "Demoted");
    Assertions.assertEquals(
        100,
        aCluster.replicas().stream().filter(r -> r.broker().id() == 2).count(),
        "Not allowed or cleared");
    Assertions.assertEquals(
        100,
        aCluster.replicas().stream().filter(r -> r.broker().id() == 3).count(),
        "Accept replicas broker demoted broker");
    Assertions.assertEquals(
        0, aCluster.replicas().stream().filter(r -> r.broker().id() == 4).count(), "Not allowed");
  }
}
