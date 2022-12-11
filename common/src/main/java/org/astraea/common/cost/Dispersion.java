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

/** Aggregate a sequence into a number */
@FunctionalInterface
public interface Dispersion {
  /**
   * Apply coefficient of variation to a series of values.
   *
   * <p>This implementation come with some assumption:
   *
   * <ul>
   *   <li>If no number was given, then the cov is zero.
   *   <li>If all numbers are zero, then the cov is zero.
   * </ul>
   */
  static Dispersion cov() {
    return numbers -> {
      if (numbers.isEmpty()) return 0;
      var numSummary = numbers.stream().mapToDouble(Number::doubleValue).summaryStatistics();
      if (numSummary.getAverage() == 0) {
        if (numSummary.getMax() == 0 && numSummary.getMin() == 0) return 0;
        else
          throw new ArithmeticException(
              "Coefficient of variation has no definition with zero average");
      }
      var numVariance =
          numbers.stream()
              .mapToDouble(Number::doubleValue)
              .map(score -> score - numSummary.getAverage())
              .map(score -> score * score)
              .summaryStatistics()
              .getAverage();
      return Math.sqrt(numVariance) / numSummary.getAverage();
    };
  }

  /**
   * Processing a series of values via a specific statistics method.
   *
   * @param scores origin data
   * @return aggregated data
   */
  double calculate(Collection<? extends Number> scores);
}
