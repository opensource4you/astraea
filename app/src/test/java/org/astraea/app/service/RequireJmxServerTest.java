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
package org.astraea.app.service;

import java.util.Set;
import org.astraea.app.metrics.jmx.BeanQuery;
import org.astraea.app.metrics.jmx.MBeanClient;
import org.astraea.app.metrics.kafka.KafkaMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RequireJmxServerTest extends RequireBrokerCluster {

  @Test
  void testQueryBeans() {
    testQueryBeans(MBeanClient.jndi(jmxServiceURL().getHost(), jmxServiceURL().getPort()));
    testQueryBeans(MBeanClient.of(jmxServiceURL()));
  }

  private void testQueryBeans(MBeanClient client) {
    try (client) {
      var result = client.queryBeans(BeanQuery.all());
      Assertions.assertFalse(result.isEmpty());
    }
  }

  @Test
  void testMemory() throws Exception {
    testMemory(MBeanClient.jndi(jmxServiceURL().getHost(), jmxServiceURL().getPort()));
    testMemory(MBeanClient.of(jmxServiceURL()));
  }

  private void testMemory(MBeanClient client) {
    try (client) {
      var memory = KafkaMetrics.Host.jvmMemory(client);
      Assertions.assertNotEquals(0, memory.heapMemoryUsage().getMax());
    }
  }

  @Test
  void testHost() {
    var legalChars = Set.of('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.');
    var address = jmxServiceURL().getHost();
    Assertions.assertNotEquals("127.0.0.1", address);
    Assertions.assertNotEquals("0.0.0.0", address);
    for (var i = 0; i < address.length(); i++) {
      Assertions.assertTrue(legalChars.contains(address.charAt(i)));
    }
  }
}
