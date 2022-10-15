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
package org.astraea.app.web;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;

public class TopicHandlerForProbabilityTest extends RequireBrokerCluster {
  @RepeatedTest(2)
  void testCreateTopicByProbability() throws ExecutionException, InterruptedException {
    var topicName = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new TopicHandler(admin);
      var request =
          Channel.ofRequest(
              PostRequest.of(
                  String.format(
                      "{\"topics\":[{\"name\":\"%s\", \"partitions\":30, \"probability\": 0.15}]}",
                      topicName)));
      var topics = handler.post(request).toCompletableFuture().get();
      Assertions.assertEquals(1, topics.topics.size());
      Utils.waitFor(
          () ->
              Utils.packException(
                          () ->
                              (TopicHandler.TopicInfo)
                                  handler
                                      .get(Channel.ofTarget(topicName))
                                      .toCompletableFuture()
                                      .get())
                      .partitions
                      .size()
                  == 30);
      var groupByBroker =
          ((TopicHandler.TopicInfo)
                  handler.get(Channel.ofTarget(topicName)).toCompletableFuture().get())
              .partitions.stream()
                  .flatMap(p -> p.replicas.stream())
                  .collect(Collectors.groupingBy(r -> r.broker));
      // those brokers should host different number of partitions, broker0 holds the most replicas,
      // followed by broker1, and lastly broker2
      var numberOfReplicas =
          groupByBroker.values().stream().map(List::size).collect(Collectors.toList());
      Assertions.assertTrue(numberOfReplicas.get(0) > numberOfReplicas.get(1));
      if (numberOfReplicas.size() == 3)
        Assertions.assertTrue(numberOfReplicas.get(1) > numberOfReplicas.get(2));
    }
  }
}
