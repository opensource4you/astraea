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
package org.astraea.app.cost;

import java.util.Map;

/** Return type of cost function, `HasBrokerCost`. It returns the score of brokers. */
public interface BrokerCost {
  /**
   * A new BrokerCost has been normalized.
   *
   * @param normalizer
   * @return A normalized BrokerCost.
   */
  default BrokerCost normalize(Normalizer normalizer) {
    var map = this.value();
    var keys = map.keySet().iterator();
    var normalization = normalizer.normalize(map.values()).iterator();
    while (normalization.hasNext()) {
      map.replace(keys.next(), normalization.next());
    }
    return () -> map;
  }

  /** @return broker-id and its "cost" */
  Map<Integer, Double> value();
}
