package org.astraea.app.cost;

import java.util.Map;

/** Return type of cost function, `HasBrokerCost`. It returns the score of brokers. */
public interface BrokerCost {
  /** @return broker-id and its "cost" */
  Map<Integer, Double> value();
}
