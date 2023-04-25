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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.astraea.common.admin.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CostAwareAssignorTest {

  @Test
  void testGreedyAssign() {
    var assign = Combinator.greedy();
    var reassign = Shuffler.incompatible();
    var subscription =
        Map.of(
            "c1",
            new SubscriptionInfo(List.of("t1", "t2", "t3"), null),
            "c2",
            new SubscriptionInfo(List.of("t1", "t2", "t3"), null));
    var cost1 =
        Map.of(
            TopicPartition.of("t1", 0),
            0.2,
            TopicPartition.of("t1", 1),
            0.4,
            TopicPartition.of("t2", 0),
            0.2,
            TopicPartition.of("t3", 0),
            0.4);

    var assignment = assign.combine(subscription, cost1);
    Assertions.assertEquals(2, assignment.size());
    assignment.forEach((c, a) -> Assertions.assertEquals(2, a.size()));
    var finalAssignment = reassign.shuffle(subscription, assignment, Map.of(), cost1);
    Assertions.assertEquals(assignment, finalAssignment);

    var incompatibility =
        Map.of(
            TopicPartition.of("t1", 0),
            Set.of(TopicPartition.of("t3", 0)),
            TopicPartition.of("t1", 1),
            Set.of(TopicPartition.of("t2", 0)),
            TopicPartition.of("t2", 0),
            Set.of(TopicPartition.of("t1", 1)),
            TopicPartition.of("t3", 0),
            Set.of(TopicPartition.of("t1", 0)));

    assignment = assign.combine(subscription, cost1);
    Assertions.assertEquals(2, assignment.size());
    finalAssignment = reassign.shuffle(subscription, assignment, incompatibility, cost1);
    finalAssignment.forEach(
        (c, a) -> {
          a.forEach(
              tp -> {
                incompatibility.get(tp).forEach(itp -> Assertions.assertFalse(a.contains(itp)));
              });
        });

    Function<String, TopicPartition> create = TopicPartition::of;
    var allIncompatible =
        Map.of(
            create.apply("t1-0"),
            Set.of(create.apply("t1-1"), create.apply("t2-0"), create.apply("t3-0")),
            create.apply("t1-1"),
            Set.of(create.apply("t1-0"), create.apply("t2-0"), create.apply("t3-0")),
            create.apply("t2-0"),
            Set.of(create.apply("t1-0"), create.apply("t1-1"), create.apply("t3-0")),
            create.apply("t3-0"),
            Set.of(create.apply("t1-0"), create.apply("t2-0"), create.apply("t1-1")));

    assignment = assign.combine(subscription, cost1);
    finalAssignment = reassign.shuffle(subscription, assignment, incompatibility, cost1);
    finalAssignment.forEach((c, a) -> Assertions.assertEquals(2, a.size()));
  }
}
