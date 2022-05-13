package org.astraea.web;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.astraea.admin.Admin;
import org.astraea.admin.Config;

class BrokerHandler implements Handler {

  private final Admin admin;

  BrokerHandler(Admin admin) {
    this.admin = admin;
  }

  private static void mustBeNumber(Optional<String> target) {
    try {
      target.ifPresent(Integer::valueOf);
    } catch (NumberFormatException e) {
      throw new NoSuchElementException("broker: " + target.get() + " does not exist");
    }
  }

  @Override
  public JsonObject response(Optional<String> target, Map<String, String> queries) {
    mustBeNumber(target);
    Predicate<Map.Entry<Integer, ?>> brokerId =
        e -> target.stream().map(Integer::valueOf).allMatch(t -> t.equals(e.getKey()));
    var brokers =
        admin.brokers().entrySet().stream()
            .filter(brokerId)
            .map(e -> new Broker(e.getKey(), e.getValue()))
            .collect(Collectors.toUnmodifiableList());

    if (target.isPresent() && brokers.size() == 1) return brokers.get(0);
    else if (target.isPresent())
      throw new NoSuchElementException("broker: " + target.get() + " does not exist");
    else return new Brokers(brokers);
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
