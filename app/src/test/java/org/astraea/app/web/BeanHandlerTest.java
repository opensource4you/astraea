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
import java.util.Optional;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BeanHandlerTest {

  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testBeans() {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(10).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));
      var handler = new BeanHandler(admin, name -> Optional.of(SERVICE.jmxServiceURL().getPort()));
      var response =
          Assertions.assertInstanceOf(
              BeanHandler.NodeBeans.class, handler.get(Channel.EMPTY).toCompletableFuture().join());
      Assertions.assertNotEquals(0, response.nodeBeans.size());

      var response1 =
          Assertions.assertInstanceOf(
              BeanHandler.NodeBeans.class,
              handler.get(Channel.ofTarget("kafka.server")).toCompletableFuture().join());
      Assertions.assertNotEquals(0, response1.nodeBeans.size());

      var response2 =
          Assertions.assertInstanceOf(
              BeanHandler.NodeBeans.class,
              handler.get(Channel.ofQueries(Map.of("topic", topic))).toCompletableFuture().join());
      Assertions.assertNotEquals(0, response2.nodeBeans.size());
    }
  }
}
