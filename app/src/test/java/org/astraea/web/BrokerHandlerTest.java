package org.astraea.web;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.astraea.Utils;
import org.astraea.admin.Admin;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BrokerHandlerTest extends RequireBrokerCluster {

  @Test
  void testListBrokers() throws InterruptedException {
    var topic = Utils.randomString(10);
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(10).create();
      TimeUnit.SECONDS.sleep(2);
      var handler = new BrokerHandler(admin);
      var response =
          Assertions.assertInstanceOf(
              BrokerHandler.Brokers.class, handler.get(Optional.empty(), Map.of()));
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
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new BrokerHandler(admin);
      Assertions.assertThrows(
          NoSuchElementException.class, () -> handler.get(Optional.of("99999"), Map.of()));
    }
  }

  @Test
  void testQueryInvalidBroker() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new BrokerHandler(admin);
      Assertions.assertThrows(
          NoSuchElementException.class, () -> handler.get(Optional.of("abc"), Map.of()));
    }
  }

  @Test
  void testQuerySingleBroker() throws InterruptedException {
    var topic = Utils.randomString(10);
    var brokerId = brokerIds().iterator().next();
    try (Admin admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(10).create();
      TimeUnit.SECONDS.sleep(2);
      var handler = new BrokerHandler(admin);
      var broker =
          Assertions.assertInstanceOf(
              BrokerHandler.Broker.class,
              handler.get(Optional.of(String.valueOf(brokerId)), Map.of()));
      Assertions.assertEquals(brokerId, broker.id);
      Assertions.assertNotEquals(0, broker.configs.size());
      Assertions.assertTrue(broker.topics.stream().anyMatch(t -> t.topic.equals(topic)));
    }
  }

  @Test
  void testBrokers() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new BrokerHandler(admin);
      Assertions.assertEquals(
          Set.of(brokerIds().iterator().next()),
          handler.brokers(Optional.of(String.valueOf(brokerIds().iterator().next()))));
      Assertions.assertEquals(brokerIds(), handler.brokers(Optional.empty()));
      Assertions.assertThrows(
          NoSuchElementException.class, () -> handler.brokers(Optional.of("aaa")));
    }
  }
}
