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
package org.astraea.app.cost.topic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.astraea.app.admin.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CalculateUtilsTest {
  static Map<Integer, Map<TopicPartition, Integer>> fakeBrokerPartitionSize = new HashMap<>();
  static Map<String, Integer> fakeRetentionMillis = new HashMap<>();
  static Map<Integer, Map<TopicPartition, Double>> fakePartitionLoad = new HashMap<>();

  @BeforeAll
  static void setup() {
    // set partition size and retention time
    Map<TopicPartition, Integer> fakePartitionSize = new HashMap<>();
    fakePartitionSize.put(TopicPartition.of("test0", 0), 100000000);
    fakePartitionSize.put(TopicPartition.of("test0", 1), 200000000);
    fakePartitionSize.put(TopicPartition.of("test0", 2), 300000000);
    fakePartitionSize.put(TopicPartition.of("test0", 3), 400000000);
    fakeBrokerPartitionSize.put(0, fakePartitionSize);
    fakePartitionSize = new HashMap<>();
    fakePartitionSize.put(TopicPartition.of("test1", 0), 500000000);
    fakePartitionSize.put(TopicPartition.of("test1", 1), 600000000);
    fakePartitionSize.put(TopicPartition.of("test1", 2), 700000000);
    fakePartitionSize.put(TopicPartition.of("test1", 3), 800000000);
    fakeBrokerPartitionSize.put(1, fakePartitionSize);
    fakeRetentionMillis.put("test0", 604800000);
    fakeRetentionMillis.put("test1", 604800000);

    // set partition load
    fakePartitionLoad = new HashMap<>();
    Map<TopicPartition, Double> fakeTopicLoad = new HashMap<>();
    fakeTopicLoad.put(TopicPartition.of("test0", 0), 0.1);
    fakeTopicLoad.put(TopicPartition.of("test0", 1), 0.2);
    fakeTopicLoad.put(TopicPartition.of("test0", 2), 0.3);
    fakeTopicLoad.put(TopicPartition.of("test0", 3), 0.4);
    fakePartitionLoad.put(0, fakeTopicLoad);
    fakeTopicLoad = new HashMap<>();
    fakeTopicLoad.put(TopicPartition.of("test1", 0), 1.5);
    fakeTopicLoad.put(TopicPartition.of("test1", 1), 1.6);
    fakeTopicLoad.put(TopicPartition.of("test1", 2), 1.7);
    fakeTopicLoad.put(TopicPartition.of("test1", 3), 1.8);
    fakePartitionLoad.put(1, fakeTopicLoad);
  }

  @Test
  void testGetLoad() {
    var Load = CalculateUtils.getLoad(fakeBrokerPartitionSize, fakeRetentionMillis);
    assertEquals(2, Load.size());
    assertEquals(4, Load.get(0).size());
    assertEquals(4, Load.get(1).size());
    assertEquals(0.17, round(Load.get(0).get(TopicPartition.of("test0", 0))));
    assertEquals(0.33, round(Load.get(0).get(TopicPartition.of("test0", 1))));
    assertEquals(0.50, round(Load.get(0).get(TopicPartition.of("test0", 2))));
    assertEquals(0.66, round(Load.get(0).get(TopicPartition.of("test0", 3))));
    assertEquals(0.83, round(Load.get(1).get(TopicPartition.of("test1", 0))));
    assertEquals(0.99, round(Load.get(1).get(TopicPartition.of("test1", 1))));
    assertEquals(1.16, round(Load.get(1).get(TopicPartition.of("test1", 2))));
    assertEquals(1.32, round(Load.get(1).get(TopicPartition.of("test1", 3))));
  }

  @Test
  void testGetScore() {
    var Score = CalculateUtils.getScore(fakePartitionLoad);
    assertEquals(2, Score.size());
    assertEquals(4, Score.get(0).size());
    assertEquals(4, Score.get(1).size());
    assertEquals(0.0, round(Score.get(0).get(TopicPartition.of("test0", 0))));
    assertEquals(0.0, round(Score.get(0).get(TopicPartition.of("test0", 1))));
    assertEquals(0.0, round(Score.get(0).get(TopicPartition.of("test0", 2))));
    assertEquals(0.0, round(Score.get(0).get(TopicPartition.of("test0", 3))));
    assertEquals(-80.50, round(Score.get(1).get(TopicPartition.of("test1", 0))));
    assertEquals(-26.83, round(Score.get(1).get(TopicPartition.of("test1", 1))));
    assertEquals(26.83, round(Score.get(1).get(TopicPartition.of("test1", 2))));
    assertEquals(80.50, round(Score.get(1).get(TopicPartition.of("test1", 3))));
  }

  double round(double score) {
    return Math.round(100 * score) / 100.0;
  }
}
