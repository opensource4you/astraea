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
package org.astraea.common.metrics.collector;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.HasBeanObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FixedMetricsTest {

  @Test
  void testFixedReceiverOnce() {
    var beans =
        Map.of(1, (Collection<BeanObject>) List.of(new BeanObject("domain", Map.of(), Map.of())));
    try (var receiver = MetricStore.Receiver.fixed(beans)) {
      Assertions.assertEquals(beans, receiver.receive(Duration.ZERO));
      Assertions.assertEquals(Map.of(), receiver.receive(Duration.ZERO));
      Assertions.assertEquals(Map.of(), receiver.receive(Duration.ZERO));
      Assertions.assertEquals(Map.of(), receiver.receive(Duration.ZERO));
    }

    var receiver = MetricStore.Receiver.fixed(beans);
    receiver.close();
    Assertions.assertEquals(Map.of(), receiver.receive(Duration.ZERO));
  }

  @Test
  void testFixedReceiver() {
    interface MyBeanObject extends HasBeanObject {}

    var beans =
        Map.of(
            1,
            (Collection<BeanObject>)
                List.of(new BeanObject("domain", Map.of("Hello", "World"), Map.of())));
    var sensor =
        (MetricSensor)
            (client, cb) ->
                List.of(
                    (MyBeanObject)
                        () ->
                            client.bean(
                                BeanQuery.builder()
                                    .domainName("domain")
                                    .property("Hello", "World")
                                    .build()));

    try (var store =
        MetricStore.builder()
            .receivers(List.of(MetricStore.Receiver.fixed(beans)))
            .beanExpiration(Duration.ofDays(3))
            .sensorsSupplier(() -> Map.of(sensor, (a, x) -> {}))
            .build()) {
      store.wait(cb -> cb.all().containsKey(1), Duration.ofSeconds(1));
      Assertions.assertEquals(Set.of(1), store.clusterBean().all().keySet());
      Assertions.assertEquals(1, store.clusterBean().all().get(1).size());
      Assertions.assertInstanceOf(
          MyBeanObject.class, store.clusterBean().all().get(1).iterator().next());
    }
  }
}
