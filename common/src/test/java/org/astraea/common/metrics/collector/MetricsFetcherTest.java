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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.astraea.common.Utils;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.MBeanClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class MetricsFetcherTest {

  @Test
  void testPublishAndClose() {
    var beans = List.of(new BeanObject(Utils.randomString(), Map.of(), Map.of()));
    var client = Mockito.mock(MBeanClient.class);
    Mockito.when(client.beans(Mockito.any())).thenReturn(beans);
    var sender = Mockito.mock(MetricsFetcher.Sender.class);
    var queue = new ConcurrentHashMap<Integer, Collection<BeanObject>>();
    Mockito.when(sender.send(Mockito.anyInt(), Mockito.any()))
        .thenAnswer(
            invocation -> {
              queue.put(
                  invocation.getArgument(0, Integer.class),
                  invocation.getArgument(1, Collection.class));
              return CompletableFuture.completedStage(null);
            });
    try (var fetcher =
        MetricsFetcher.builder()
            .sender(sender)
            .clientSupplier(() -> Map.of(-1000, client))
            .fetchBeanDelay(Duration.ofSeconds(1))
            .build()) {
      Utils.sleep(Duration.ofSeconds(3));
      Assertions.assertEquals(Set.of(-1000), fetcher.clientIds());
      Assertions.assertNotEquals(0, queue.size());
      queue.values().forEach(es -> Assertions.assertEquals(beans, es));

      var latest = fetcher.latest();
      Assertions.assertEquals(1, latest.size());
      latest.values().forEach(bs -> Assertions.assertEquals(beans, bs));
    }
    // make sure client get closed
    Mockito.verify(client, Mockito.times(1)).close();
    // make sure sender get closed
    Mockito.verify(sender, Mockito.times(1)).close();
  }

  @Test
  void testNullCheck() {
    var builder = MetricsFetcher.builder();
    Assertions.assertThrows(NullPointerException.class, builder::build);
    builder.sender(MetricsFetcher.Sender.local());
    Assertions.assertThrows(NullPointerException.class, builder::build);
    builder.clientSupplier(Map::of);
    var fetcher = builder.build();
    fetcher.close();
  }

  @Test
  void testFetchBeanDelay() {
    var client = Mockito.mock(MBeanClient.class);
    try (var fetcher =
        MetricsFetcher.builder()
            .sender(MetricsFetcher.Sender.local())
            .clientSupplier(() -> Map.of(-1000, client))
            .fetchBeanDelay(Duration.ofSeconds(1000))
            .build()) {
      Utils.sleep(Duration.ofSeconds(3));
      Assertions.assertEquals(1, fetcher.clientIds().size());
      Assertions.assertEquals(0, fetcher.latest().size());
      // make sure client is not called
      Mockito.verify(client, Mockito.never()).beans(Mockito.any());
    }
  }

  @Test
  void testFetchMetadataDelay() {
    var client = Mockito.mock(MBeanClient.class);
    Supplier<Map<Integer, MBeanClient>> supplier = Mockito.mock(Supplier.class);
    Mockito.when(supplier.get()).thenReturn(Map.of(-1000, client));
    try (var fetcher =
        MetricsFetcher.builder()
            .sender(MetricsFetcher.Sender.local())
            .clientSupplier(supplier)
            .fetchMetadataDelay(Duration.ofSeconds(1000))
            .build()) {
      Utils.sleep(Duration.ofSeconds(3));
      // the metadata is get updated immediately
      Assertions.assertEquals(1, fetcher.clientIds().size());
      // the delay is too larger to see next update
      Mockito.verify(supplier, Mockito.times(1)).get();
    }
  }
}
