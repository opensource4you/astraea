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
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.astraea.common.producer.Producer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BalancerUtilsIntegratedTest extends RequireBrokerCluster {

  @Test
  void testUpdate() {
    var topicName = Utils.randomString(5);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).create();
      Utils.sleep(Duration.ofSeconds(3));

      try (var producer = Producer.of(bootstrapServers())) {
        IntStream.range(0, 100)
            .forEach(ignored -> producer.sender().topic(topicName).key(new byte[10]).run());
      }

      var clusterInfo = admin.clusterInfo(Set.of(topicName));
      clusterInfo.replicas().forEach(r -> Assertions.assertTrue(r.size() > 0));

      var replica = clusterInfo.replicas().iterator().next();
      var newBrokerId =
          brokerIds().stream().filter(id -> id != replica.nodeInfo().id()).findFirst().get();

      var randomSizeValue = ThreadLocalRandom.current().nextInt();
      var merged =
          BalancerUtils.update(
              clusterInfo,
              ClusterLogAllocation.of(
                  List.of(
                      Replica.of(
                          topicName,
                          0,
                          NodeInfo.of(newBrokerId, "", -1),
                          0,
                          randomSizeValue,
                          true,
                          true,
                          false,
                          false,
                          true,
                          replica.path()))));

      Assertions.assertEquals(clusterInfo.replicas().size(), merged.replicas().size());
      Assertions.assertEquals(clusterInfo.topics().size(), merged.topics().size());
      merged.replicas().forEach(r -> Assertions.assertEquals(randomSizeValue, r.size()));
      Assertions.assertEquals(1, merged.replicas(topicName).size());
      Assertions.assertEquals(newBrokerId, merged.replicas(topicName).get(0).nodeInfo().id());
    }
  }
}
