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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.common.admin.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CostAwareAssignorTest {

  @Test
  void testCombineAndShuffle() {
    var combinator = Combinator.greedy();
    var shuffler = Shuffler.incompatible(Duration.ofSeconds(1));
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

    var assignment = combinator.combine(subscription, cost);
    var shuffledAssignment = shuffler.shuffle(subscription, assignment, Map.of(), cost);
    Assertions.assertEquals(assignment, shuffledAssignment);

    var incompatibility =
        Map.of(
            TopicPartition.of("t1", 1),
            Set.of(TopicPartition.of("t3", 0)),
            TopicPartition.of("t3", 0),
            Set.of(TopicPartition.of("t1", 1)));
    assignment =
        Map.of(
            "c1",
            List.of(TopicPartition.of("t1-0"), TopicPartition.of("t1-1")),
            "c2",
            List.of(TopicPartition.of("t2-0"), TopicPartition.of("t3-0")));
    shuffledAssignment = shuffler.shuffle(subscription, assignment, incompatibility, cost);
    Assertions.assertEquals(assignment, shuffledAssignment);
  }
}
