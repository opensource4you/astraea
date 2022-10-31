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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BrokerHandlerTest extends RequireBrokerCluster {

  @Test
  void testListBrokers() {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(10).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));
      var handler = new BrokerHandler(admin);
      var response =
          Assertions.assertInstanceOf(
              BrokerHandler.Brokers.class, handler.get(Channel.EMPTY).toCompletableFuture().join());
      Assertions.assertEquals(brokerIds().size(), response.brokers.size());
      brokerIds()
          .forEach(
              id ->
                  Assertions.assertEquals(
                      1, response.brokers.stream().filter(b -> b.id == id).count()));
      response.brokers.forEach(
          b -> Assertions.assertTrue(b.topics.stream().anyMatch(t -> t.topic.equals(topic))));
    }
  }

  @Test
  void testQueryNonexistentBroker() {
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BrokerHandler(admin);
      Assertions.assertInstanceOf(
          NoSuchElementException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () -> handler.get(Channel.ofTarget("99999")).toCompletableFuture().join())
              .getCause());
    }
  }

  @Test
  void testQueryInvalidBroker() {
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BrokerHandler(admin);
      Assertions.assertInstanceOf(
          NoSuchElementException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () -> handler.get(Channel.ofTarget("abc")).toCompletableFuture().join())
              .getCause());
    }
  }

  @Test
  void testQuerySingleBroker() {
    var topic = Utils.randomString(10);
    var brokerId = brokerIds().iterator().next();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(10).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(2));
      var handler = new BrokerHandler(admin);
      var broker =
          Assertions.assertInstanceOf(
              BrokerHandler.Broker.class,
              handler.get(Channel.ofTarget(String.valueOf(brokerId))).toCompletableFuture().join());
      Assertions.assertEquals(brokerId, broker.id);
      Assertions.assertNotEquals(0, broker.configs.size());
      Assertions.assertTrue(broker.topics.stream().anyMatch(t -> t.topic.equals(topic)));
    }
  }

  @Test
  void testBrokers() {
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BrokerHandler(admin);
      Assertions.assertEquals(
          Set.of(brokerIds().iterator().next()),
          handler
              .brokers(Optional.of(String.valueOf(brokerIds().iterator().next())))
              .toCompletableFuture()
              .join());
      Assertions.assertEquals(
          brokerIds(), handler.brokers(Optional.empty()).toCompletableFuture().join());
      Assertions.assertThrows(
          CompletionException.class,
          () -> handler.brokers(Optional.of("aaa")).toCompletableFuture().join());
    }
  }
}
