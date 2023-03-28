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
package org.astraea.gui.tab;

import java.time.Duration;
import java.util.Set;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ClientNodeTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(1).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testConsumerResult() {
    var group = Utils.randomString();
    var topic = Utils.randomString();
    try (var admin = Admin.of(SERVICE.bootstrapServers());
        var producer = Producer.of(SERVICE.bootstrapServers());
        var consumer =
            Consumer.forTopics(Set.of(topic))
                .config(ConsumerConfigs.GROUP_ID_CONFIG, group)
                .bootstrapServers(SERVICE.bootstrapServers())
                .build()) {
      producer
          .send(Record.builder().topic(topic).key(new byte[100]).build())
          .toCompletableFuture()
          .join();
      var records = consumer.poll(Duration.ofSeconds(10));
      Assertions.assertEquals(0, records.size());

      var cgs = admin.consumerGroups(Set.of(group)).toCompletableFuture().join();
      Assertions.assertEquals(1, cgs.size());

      var ps = admin.partitions(Set.of(topic)).toCompletableFuture().join();
      Assertions.assertEquals(1, ps.size());

      var result = ClientNode.consumerResult(cgs, ps);
      Assertions.assertEquals(1, result.size(), "result: " + result);
      Assertions.assertEquals(group, result.get(0).get("group"));
      Assertions.assertNotNull(result.get(0).get("assignor"));
      Assertions.assertNotNull(result.get(0).get("state"));
      Assertions.assertEquals(
          SERVICE.dataFolders().entrySet().iterator().next().getKey(),
          result.get(0).get("coordinator"));
      Assertions.assertEquals(topic, result.get(0).get("topic"));
    }
  }
}
