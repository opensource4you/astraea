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
package org.astraea.common.balancer.reports;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Offer a series of report functions for the balancer to invoke when a specific event occurs. The
 * implementation of {@link BalancerProgressReport} should fill in the action to perform when the
 * specific event is triggered. Maybe write the value to a file or publish this result via JMX
 * Mbeans. If there are multiple {@link BalancerProgressReport} a balancer has to react to, consider
 * use {@link BalancerProgressReport#merge(Collection)} to compose multiple reporters into one.
 *
 * <p>All the method implementation should be thread-safe.
 */
public interface BalancerProgressReport {

  /**
   * Merge multiple {@link BalancerProgressReport} into one. The returned implementation will
   * broadcast the given value to all the specified {@link BalancerProgressReport} instances.
   */
  static BalancerProgressReport merge(Collection<BalancerProgressReport> reports) {
    // create defensive copy, and filter out any EMPTY instances
    final var defensiveCopy =
        reports.stream()
            .filter(instance -> instance != EMPTY)
            .collect(Collectors.toUnmodifiableList());

    // if there is no meaningful reporter, just return the EMPTY one
    if (defensiveCopy.isEmpty()) return EMPTY;

    return new BalancerProgressReport() {
      @Override
      public void iteration(long time, double clusterCost) {
        defensiveCopy.forEach(r -> r.iteration(time, clusterCost));
      }
    };
  }

  /** a {@link BalancerProgressReport} that does nothing whichever function gets called. */
  BalancerProgressReport EMPTY = new BalancerProgressReport() {};

  /**
   * Notify that iteration result of the algorithm, it calculated a new allocation with a specific
   * cluster cost score on it.
   *
   * <p>Since this method will expose the intermediate result of the algorithm. The definition of
   * what allocation can be considered as an intermediate result is up to the implementation detail
   * to decide. This might lead us to that we can't effectively compare the score behavior expressed
   * by two different algorithms. For example: For Hill-Climb Algorithm, each step it moves can be
   * considered as a new allocation. But with Genetic Algorithm, we might have many allocations on
   * hand, we perform some operation on the population can generate another set of population. At
   * this point we have many feasible allocations here. Should Genetic Algorithm publish every
   * individual in the population or just the best individual? It is all up to the implementation to
   * decide. The person who evaluates those numbers must be aware of this pitfall.
   *
   * @param time when does this iteration finished, measured as {@link System#currentTimeMillis()}
   * @param clusterCost the cluster cost published by this iteration.
   */
  default void iteration(long time, double clusterCost) {}
}
