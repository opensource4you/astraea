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
package org.astraea.common.assignor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.astraea.common.admin.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GeneratorTest {
  @Test
  void testCombinator() {
    var checkSubscriptionAndAssignment =
        (BiFunction<Map<String, List<TopicPartition>>, Map<String, SubscriptionInfo>, Boolean>)
            (assignment, subscription) ->
                assignment.entrySet().stream()
                    .allMatch(
                        e ->
                            e.getValue().stream()
                                .allMatch(
                                    tp ->
                                        subscription
                                            .get(e.getKey())
                                            .topics()
                                            .contains(tp.topic())));

    // test same subscription
    var subscription =
        Map.of(
            "c1",
            new SubscriptionInfo(List.of("t1", "t2", "t3"), null),
            "c2",
            new SubscriptionInfo(List.of("t1", "t2", "t3"), null));
    var cost =
        Map.of(
            TopicPartition.of("t1", 0),
            0.2,
            TopicPartition.of("t1", 1),
            0.4,
            TopicPartition.of("t2", 0),
            0.2,
            TopicPartition.of("t3", 0),
            0.4);
    var randomGenerator =
        Generator.randomGenerator(subscription, cost, Hint.lowCostHint(subscription, cost));
    var result = randomGenerator.get();
    var partitionsInResult = result.values().stream().flatMap(Collection::stream).toList();
    var partitionInCluster = cost.keySet();
    Assertions.assertEquals(partitionInCluster.size(), partitionsInResult.size());
    Assertions.assertTrue(partitionsInResult.containsAll(partitionInCluster));
    Assertions.assertTrue(checkSubscriptionAndAssignment.apply(result, subscription));
    result.forEach((c, tps) -> Assertions.assertFalse(tps.isEmpty()));

    // Validate that unsubscribed partitions are not be assigned to the consumer.
    subscription =
        Map.of(
            "c1",
            new SubscriptionInfo(List.of("t1"), null),
            "c2",
            new SubscriptionInfo(List.of("t1", "t2", "t3"), null));
    randomGenerator =
        Generator.randomGenerator(subscription, cost, Hint.lowCostHint(subscription, cost));
    result = randomGenerator.get();
    partitionsInResult = result.values().stream().flatMap(Collection::stream).toList();

    Assertions.assertEquals(partitionInCluster.size(), partitionsInResult.size());
    Assertions.assertTrue(partitionsInResult.containsAll(partitionInCluster));
    Assertions.assertTrue(checkSubscriptionAndAssignment.apply(result, subscription));

    // Every consumer get at least one partition
    subscription =
        Map.of(
            "c1",
            new SubscriptionInfo(List.of("t1", "t2", "t3"), null),
            "c2",
            new SubscriptionInfo(List.of("t1", "t2", "t3"), null),
            "c3",
            new SubscriptionInfo(List.of("t1", "t2", "t3"), null),
            "c4",
            new SubscriptionInfo(List.of("t1", "t2", "t3"), null));
    randomGenerator =
        Generator.randomGenerator(subscription, cost, Hint.lowCostHint(subscription, cost));
    result = randomGenerator.get();
    result.forEach((c, tps) -> Assertions.assertEquals(1, tps.size()));

    // Validate there is no consumer was assigned more than 1 partition
    cost =
        Map.of(
            TopicPartition.of("t1", 0),
            0.2,
            TopicPartition.of("t2", 0),
            0.2,
            TopicPartition.of("t3", 0),
            0.4);
    randomGenerator =
        Generator.randomGenerator(subscription, cost, Hint.lowCostHint(subscription, cost));
    result = randomGenerator.get();
    result.forEach((c, tps) -> Assertions.assertTrue(tps.size() <= 1));

    subscription =
        Map.of(
            "c1",
            new SubscriptionInfo(List.of("t1"), null),
            "c2",
            new SubscriptionInfo(List.of("t2"), null),
            "c3",
            new SubscriptionInfo(List.of("t3"), null));
    randomGenerator =
        Generator.randomGenerator(subscription, cost, Hint.lowCostHint(subscription, cost));
    result = randomGenerator.get();
    Assertions.assertTrue(checkSubscriptionAndAssignment.apply(result, subscription));
  }
}
