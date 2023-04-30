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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.astraea.common.Utils;
import org.astraea.common.metrics.BeanObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class MetricStoreTest {

  @Test
  void testReceiveAndClose() {
    var bean = new BeanObject(Utils.randomString(), Map.of(), Map.of());
    var queue = new LinkedBlockingQueue<Map<Integer, Collection<BeanObject>>>();
    queue.add(Map.of(1000, List.of(bean)));
    var receiver = Mockito.mock(MetricStore.Receiver.class);
    Mockito.when(receiver.receive(Mockito.any()))
        .thenAnswer(invocation -> Utils.packException(queue::take));
    try (var store =
        MetricStore.builder().receiver(receiver).beanExpiration(Duration.ofSeconds(100)).build()) {
      Utils.sleep(Duration.ofSeconds(3));
      Assertions.assertEquals(1, store.clusterBean().all().size());
      Assertions.assertEquals(Set.of(1000), store.clusterBean().all().keySet());
      Assertions.assertEquals(1, store.clusterBean().all().get(1000).size());
      Assertions.assertEquals(
          bean, store.clusterBean().all().get(1000).iterator().next().beanObject());
    }
    // make sure receiver get closed
    Mockito.verify(receiver, Mockito.times(1)).close();
  }

  @Test
  void testNullCheck() {
    var builder = MetricStore.builder();
    Assertions.assertThrows(NullPointerException.class, builder::build);
    builder.receiver(timeout -> Map.of());
    var store = builder.build();
    store.close();
  }

  @Test
  void testBeanExpiration() {
    var queue = new LinkedBlockingQueue<Map<Integer, Collection<BeanObject>>>();
    queue.add(
        Map.of(
            1000,
            List.of(new BeanObject(Utils.randomString(), Map.of(), Map.of())),
            1002,
            List.of(new BeanObject(Utils.randomString(), Map.of(), Map.of())),
            1003,
            List.of(new BeanObject(Utils.randomString(), Map.of(), Map.of()))));
    var count = new AtomicInteger(0);
    try (var store =
        MetricStore.builder()
            .receiver(
                timeout -> {
                  count.incrementAndGet();
                  return Utils.packException(queue::take);
                })
            .beanExpiration(Duration.ofSeconds(5))
            .build()) {
      Utils.waitFor(() -> store.clusterBean().all().size() == 3);
      Utils.sleep(Duration.ofSeconds(5));
      Assertions.assertNotEquals(0, count.get());
      Assertions.assertEquals(0, store.clusterBean().all().size());
    }
  }
}
