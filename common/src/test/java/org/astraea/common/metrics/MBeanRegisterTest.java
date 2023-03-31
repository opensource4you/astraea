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
package org.astraea.common.metrics;

import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.astraea.common.metrics.client.producer.ProducerMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MBeanRegisterTest {

  @Test
  void testNodeMetrics() {
    MBeanRegister.local()
        .domainName("kafka.producer")
        .property("type", "producer-node-metrics")
        .property("node-id", "100")
        .property("client-id", "xxxx")
        .attribute("incoming-byte-rate", Double.class, () -> 10D)
        .register();

    var metrics = ProducerMetrics.node(MBeanClient.local());
    Assertions.assertEquals(1, metrics.size());
    Assertions.assertEquals(10D, metrics.iterator().next().incomingByteRate());
  }

  @Test
  void testBuilder() {
    try (MBeanClient client = MBeanClient.local()) {
      var domainName = MBeanRegisterTest.class.getPackageName();
      var id = UUID.randomUUID().toString();
      Supplier<BeanObject> bean =
          () -> client.bean(BeanQuery.builder().domainName(domainName).property("id", id).build());

      // register
      MBeanRegister.local()
          .domainName(domainName)
          .description("Hello World")
          .property("id", id)
          .attribute("Name", String.class, () -> "Robert")
          .attribute("Age", Integer.class, () -> 43)
          .register();
      Assertions.assertEquals(domainName, bean.get().domainName());
      Assertions.assertEquals(Map.of("id", id), bean.get().properties());
      Assertions.assertEquals(Map.of("Name", "Robert", "Age", 43), bean.get().attributes());
    }
  }
}
