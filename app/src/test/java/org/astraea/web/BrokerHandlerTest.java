package org.astraea.web;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import org.astraea.admin.Admin;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BrokerHandlerTest extends RequireBrokerCluster {

  @Test
  void testListBrokers() {
    try (Admin admin = Admin.of(bootstrapServers())) {
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
  void testQuerySingleBroker() {
    var brokerId = brokerIds().iterator().next();
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new BrokerHandler(admin);
      var broker =
          Assertions.assertInstanceOf(
              BrokerHandler.Broker.class,
              handler.get(Optional.of(String.valueOf(brokerId)), Map.of()));
      Assertions.assertEquals(brokerId, broker.id);
      Assertions.assertNotEquals(0, broker.configs.size());
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
