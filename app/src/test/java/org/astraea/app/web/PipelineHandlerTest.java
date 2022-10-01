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

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.producer.Producer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PipelineHandlerTest extends RequireBrokerCluster {

  @Test
  void testQueries() {
    // this topic-partition has no producers/consumers
    var tp = new PipelineHandler.TopicPartition("g", 1);
    Assertions.assertTrue(
        PipelineHandler.filter(Map.of(PipelineHandler.ACTIVE_KEY, "false")).test(tp));
    Assertions.assertFalse(
        PipelineHandler.filter(Map.of(PipelineHandler.ACTIVE_KEY, "true")).test(tp));
    Assertions.assertTrue(PipelineHandler.filter(Map.of()).test(tp));

    // add some producers/consumers
    tp.from.add(Mockito.mock(PipelineHandler.Producer.class));
    Assertions.assertFalse(
        PipelineHandler.filter(Map.of(PipelineHandler.ACTIVE_KEY, "false")).test(tp));
    Assertions.assertTrue(
        PipelineHandler.filter(Map.of(PipelineHandler.ACTIVE_KEY, "true")).test(tp));
    Assertions.assertTrue(PipelineHandler.filter(Map.of()).test(tp));
  }

  @Test
  void testGetPipeline() throws InterruptedException, ExecutionException {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).create();
      Utils.sleep(Duration.ofSeconds(2));
      try (var producer = Producer.of(bootstrapServers())) {
        producer.sender().topic(topic).value(new byte[10]).run().toCompletableFuture().get();
        var handler = new PipelineHandler(admin);
        var response =
            Assertions.assertInstanceOf(
                PipelineHandler.TopicPartitions.class, handler.get(Channel.EMPTY));
        Assertions.assertNotEquals(0, response.topicPartitions.size());
        Assertions.assertEquals(
            1, response.topicPartitions.stream().filter(t -> t.topic.equals(topic)).count());
      }
    }
  }
}
