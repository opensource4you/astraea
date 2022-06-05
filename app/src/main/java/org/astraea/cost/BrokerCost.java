package org.astraea.cost;

import java.util.Map;

/** Return type of cost function, `HasBrokerCost`. It returns the score of brokers. */
public interface BrokerCost {
  default BrokerCost normalize(Normalizer normalizer) {
    return () -> {
      var map = this.value();
      var normalization = normalizer.normalize(map.values()).iterator();
      map.replaceAll((broker, v) -> normalization.next());
      return map;
    };
  }

  /** @return broker-id and its "cost" */
  Map<Integer, Double> value();
}
