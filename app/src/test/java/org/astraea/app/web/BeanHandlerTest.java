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
import java.util.NoSuchElementException;
import java.util.Optional;
import org.astraea.app.admin.Admin;
import org.astraea.app.common.Utils;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BeanHandlerTest extends RequireBrokerCluster {

  @Test
  void testArgument() {
    var arg = new WebService.Argument();
    Assertions.assertFalse(arg.needJmx());
    arg.jmxPort = 1000;
    Assertions.assertTrue(arg.needJmx());
    arg.jmxPort = -1;
    arg.jmxPorts = Map.of("a", "100");
    Assertions.assertTrue(arg.needJmx());
    Assertions.assertEquals(100, arg.jmxPorts().apply("a"));
    Assertions.assertThrows(NoSuchElementException.class, () -> arg.jmxPorts().apply("b"));

    arg.jmxPort = 999;
    Assertions.assertEquals(999, arg.jmxPorts().apply("b"));
  }

  @Test
  void testBeans() {
    var topic = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(10).create();
      Utils.sleep(Duration.ofSeconds(2));
      var handler = new BeanHandler(admin, name -> jmxServiceURL().getPort());
      var response =
          Assertions.assertInstanceOf(
              BeanHandler.NodeBeans.class, handler.get(Optional.empty(), Map.of()));
      Assertions.assertNotEquals(0, response.nodeBeans.size());

      var response1 =
          Assertions.assertInstanceOf(
              BeanHandler.NodeBeans.class, handler.get(Optional.of("kafka.server"), Map.of()));
      Assertions.assertNotEquals(0, response1.nodeBeans.size());

      var response2 =
          Assertions.assertInstanceOf(
              BeanHandler.NodeBeans.class, handler.get(Optional.empty(), Map.of("topic", topic)));
      Assertions.assertNotEquals(0, response2.nodeBeans.size());
    }
  }
}
