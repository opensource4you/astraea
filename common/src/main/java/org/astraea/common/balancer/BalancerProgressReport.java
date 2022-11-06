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
package org.astraea.common.balancer;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Offer a series of report functions for the balancer to invoke when a specific event occurs. The
 * implementation of {@link BalancerProgressReport} should fill in the action to perform when the
 * specific event is triggered. Maybe write the value to a file or publish this result via JMX
 * Mbeans. If there are multiple {@link BalancerProgressReport} a balancer has to react to, consider
 * use {@link BalancerProgressReport#merge(Collection)} to compose multiple reporter into one.
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
      public void cost(long time, double value) {
        defensiveCopy.forEach(r -> r.cost(time, value));
      }
    };
  }

  /** a {@link BalancerProgressReport} that does nothing whichever function gets called. */
  BalancerProgressReport EMPTY = new BalancerProgressReport() {};

  default void cost(long time, double value) {}
}
