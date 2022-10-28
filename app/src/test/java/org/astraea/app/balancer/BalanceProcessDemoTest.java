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
package org.astraea.app.balancer;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import org.astraea.common.Utils;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Test;

public class BalanceProcessDemoTest extends RequireBrokerCluster {

  @Test
  void run() throws ExecutionException, InterruptedException {
    // prepare topics
    createTopics();

    // before operation allocation
    System.out.println("[Before]");
    describeCurrentAllocation();

    // run
    System.out.println("[Execute]");
    BalanceProcessDemo.main(new String[] {"--bootstrap.servers", bootstrapServers()});

    // after operation allocation
    System.out.println("[After]");
    describeCurrentAllocation();
  }

  void describeCurrentAllocation() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      var cla =
          ClusterLogAllocation.of(
              admin
                  .clusterInfo(admin.topicNames(false).toCompletableFuture().get())
                  .toCompletableFuture()
                  .get());
      System.out.println(ClusterLogAllocation.toString(cla));
      System.out.println();
    }
  }

  void createTopics() throws ExecutionException, InterruptedException {
    try (var admin = AsyncAdmin.of(bootstrapServers())) {
      admin
          .creator()
          .topic("TestingTopic")
          .numberOfPartitions(10)
          .numberOfReplicas((short) 2)
          .run()
          .toCompletableFuture()
          .get();
    }
    Utils.sleep(Duration.ofSeconds(1));
  }
}
