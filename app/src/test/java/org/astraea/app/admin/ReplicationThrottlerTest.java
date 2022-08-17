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
package org.astraea.app.admin;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.common.DataRate;
import org.astraea.app.common.Utils;
import org.astraea.app.producer.Producer;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

public class ReplicationThrottlerTest extends RequireBrokerCluster {

  /**
   * There is a bug in the Kafka broker implementation. This will cause the replication throttle to
   * act unstable at the beginning of replication. For more details, see <a
   * href="https://github.com/apache/kafka/pull/12528">this</a>.
   */
  @EnabledIfEnvironmentVariable(named = "RunReplicationThrottler", matches = "^yes$")
  @Test
  void runReplicationThrottler() {
    var bootstrapServer = bootstrapServers();
    try (Admin admin = Admin.of(bootstrapServer)) {
      // 1. create topic
      System.out.println("[Create topic]");
      var topicName = Utils.randomString();
      admin
          .creator()
          .topic(Utils.randomString())
          .numberOfPartitions(3)
          .numberOfReplicas((short) 2)
          .create();
      admin.creator().topic(topicName).numberOfPartitions(1).create();
      Utils.sleep(Duration.ofSeconds(1));
      admin.migrator().partition(topicName, 0).moveTo(List.of(0));

      // 2. send 200 MB data
      System.out.println("[Send data]");
      try (var producer = Producer.of(bootstrapServer)) {
        var bytes = new byte[1000];
        IntStream.range(0, 200 * 1000)
            .mapToObj(i -> producer.sender().topic(topicName).value(bytes).run())
            .collect(Collectors.toUnmodifiableList())
            .forEach(i -> i.toCompletableFuture().join());
      }

      // 3. apply throttle
      System.out.println("[Apply replication throttle]");
      admin
          .replicationThrottler()
          .ingress(DataRate.MB.of(10).perSecond())
          .egress(DataRate.MB.of(10).perSecond())
          .throttle(topicName)
          .apply();
      Utils.sleep(Duration.ofSeconds(1));

      // 4. trigger replication via migrator
      System.out.println("[Migration]");
      var start = System.currentTimeMillis();
      admin.migrator().partition(topicName, 0).moveTo(List.of(1));
      Utils.sleep(Duration.ofMillis(100));

      ReplicaSyncingMonitor.main(new String[] {"--bootstrap.servers", bootstrapServer});

      // 5. wait until it finished
      Utils.waitFor(
          () ->
              admin.replicas(Set.of(topicName)).get(TopicPartition.of(topicName, 0)).stream()
                  .filter(x -> x.broker() == 1)
                  .findFirst()
                  .map(Replica::inSync)
                  .orElse(false),
          Duration.ofSeconds(20));
      var end = System.currentTimeMillis();

      // 6. assertion
      var migrationTime = ((end - start) / 1000);
      var finishedOnTime = 17 < migrationTime && migrationTime < 24;
      System.out.println("Finish Time: " + migrationTime);
      Assertions.assertTrue(
          finishedOnTime,
          "Migration too fast or too slow? Finish Time:" + migrationTime + " second");

      // 7. clear throttle
      admin.clearReplicationThrottle(topicName);
    }
  }
}
