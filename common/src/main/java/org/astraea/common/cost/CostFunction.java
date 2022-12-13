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
package org.astraea.common.cost;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.astraea.common.Configuration;
import org.astraea.common.metrics.Sensor;
import org.astraea.common.metrics.collector.Fetcher;
import org.astraea.common.metrics.collector.MetricSensor;

/**
 * It is meaningless to implement this interface. Instead, we should implement interfaces like
 * {@link HasBrokerCost} ,{@link HasClusterCost}.
 *
 * <p>Constructors in CostFunction can only take no or only one parameter {@link Configuration}, ex.
 * Constructor({@link Configuration} configuration) or Constructor()
 *
 * <p>A cost function might take advantage of some Mbean metrics to function. One can indicate such
 * requirement in the {@link CostFunction#fetcher()} function. If the operation logic of
 * CostFunction thinks the metrics on hand are insufficient or not ready to work. One can throw a
 * {@link NoSufficientMetricsException} from the calculation logic of {@link HasBrokerCost} or
 * {@link HasClusterCost}. This serves as a hint to the caller that it needs newer metrics and
 * please retry later.
 */
public interface CostFunction {

  /**
   * @return the metrics getters. Those getters are used to fetch mbeans.
   */
  default Optional<Fetcher> fetcher() {
    return Optional.empty();
  }

  /**
   * @return the {@link Sensor} and the type of {@link org.astraea.common.metrics.stats.Stat} name
   *     to use.
   */
  default Collection<MetricSensor> sensors() {
    return List.of();
  }
}
