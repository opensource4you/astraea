package org.astraea.web;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.astraea.admin.Admin;
import org.astraea.admin.Config;

class BrokerHandler implements Handler {

  private final Admin admin;

  BrokerHandler(Admin admin) {
    this.admin = admin;
  }

  Set<Integer> brokers(Optional<String> target) {
    try {
      return Handler.compare(admin.brokerIds(), target.map(Integer::valueOf));
    } catch (NumberFormatException e) {
      throw new NoSuchElementException("the broker id must be number");
    }
  }

  @Override
  public JsonObject get(Optional<String> target, Map<String, String> queries) {
    var brokers =
        admin.brokers(brokers(target)).entrySet().stream()
            .map(e -> new Broker(e.getKey(), e.getValue()))
            .collect(Collectors.toUnmodifiableList());
    if (target.isPresent() && brokers.size() == 1) return brokers.get(0);
    return new Brokers(brokers);
  }

  static class Broker implements JsonObject {
    final int id;
    final Map<String, String> configs;

    Broker(int id, Config configs) {
      this.id = id;
      this.configs =
          StreamSupport.stream(configs.spliterator(), false)
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
  }

  static class Brokers implements JsonObject {
    final List<Broker> brokers;

    Brokers(List<Broker> brokers) {
      this.brokers = brokers;
    }
  }
}
