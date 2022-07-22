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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.astraea.app.admin.Admin;
import org.astraea.app.common.Utils;
import org.astraea.app.producer.Producer;
import org.astraea.app.producer.Serializer;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GetPartitionInfTest extends RequireBrokerCluster {
  static Admin admin;

  @BeforeAll
  static void setup() throws ExecutionException, InterruptedException {
    admin = Admin.of(bootstrapServers());
    Map<Integer, String> topicName = new HashMap<>();
    topicName.put(0, "testPartitionScore0");
    topicName.put(1, "testPartitionScore1");
    topicName.put(2, "testPartitionScore2");

    try (var admin = Admin.of(bootstrapServers())) {
      admin
          .creator()
          .topic(topicName.get(0))
          .numberOfPartitions(4)
          .numberOfReplicas((short) 1)
          .create();
      admin
          .creator()
          .topic(topicName.get(1))
          .numberOfPartitions(4)
          .numberOfReplicas((short) 1)
          .create();
      admin
          .creator()
          .topic(topicName.get(2))
          .numberOfPartitions(4)
          .numberOfReplicas((short) 1)
          .create();
      // wait for topic
      Utils.sleep(Duration.ofSeconds(5));
    }
    var producer =
        Producer.builder()
            .bootstrapServers(bootstrapServers())
            .keySerializer(Serializer.STRING)
            .build();
    int size = 10000;
    for (int t = 0; t <= 2; t++) {
      for (int p = 0; p <= 3; p++) {
        producer
            .sender()
            .topic(topicName.get(t))
            .partition(p)
            .value(new byte[size])
            .run()
            .toCompletableFuture()
            .get();
      }
      size += 10000;
    }
  }

  @Test
  void testGetSize() {
    var brokerPartitionSize = GetPartitionInf.getSize(admin);
    assertEquals(3, brokerPartitionSize.size());
    assertEquals(
        3 * 4,
        brokerPartitionSize.get(0).size()
            + brokerPartitionSize.get(1).size()
            + brokerPartitionSize.get(2).size());
  }

  @Test
  void testGetRetentionMillis() {
    var brokerPartitionRetentionMillis = GetPartitionInf.getRetentionMillis(admin);
    assertEquals(3, brokerPartitionRetentionMillis.size());
  }
}
