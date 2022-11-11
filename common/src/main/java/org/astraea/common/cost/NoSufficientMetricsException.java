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

import java.time.Duration;
import java.util.Objects;

/**
 * Indicate that {@link CostFunction} thinks the metrics on hand cannot be processed. It probably is
 * due to the insufficient amount of metrics or contradiction in the value of the metric. This
 * exception serves as a hint to the caller that it needs more or newer metrics to function, please
 * retry later.
 */
public class NoSufficientMetricsException extends RuntimeException {

  private final CostFunction source;
  private final Duration suggestedWait;

  public NoSufficientMetricsException(CostFunction source, Duration suggestedWait) {
    super();
    if (suggestedWait.isNegative() || suggestedWait.isZero())
      throw new IllegalArgumentException(
          "the wait time should be positive: " + suggestedWait.toMillis());
    this.source = Objects.requireNonNull(source);
    this.suggestedWait = Objects.requireNonNull(suggestedWait);
  }

  public NoSufficientMetricsException(CostFunction source, Duration suggestedWait, String message) {
    super(composedMessage(source, message));
    if (suggestedWait.isNegative() || suggestedWait.isZero())
      throw new IllegalArgumentException(
          "the wait time should be positive: " + suggestedWait.toMillis());
    this.source = Objects.requireNonNull(source);
    this.suggestedWait = Objects.requireNonNull(suggestedWait);
  }

  public NoSufficientMetricsException(
      CostFunction source, Duration suggestedWait, String message, Throwable cause) {
    super(composedMessage(source, message), cause);
    if (suggestedWait.isNegative() || suggestedWait.isZero())
      throw new IllegalArgumentException(
          "the wait time should be positive: " + suggestedWait.toMillis());
    this.source = Objects.requireNonNull(source);
    this.suggestedWait = Objects.requireNonNull(suggestedWait);
  }

  /** Which cost function cause this exception. */
  public CostFunction source() {
    return source;
  }

  /**
   * The suggested retry interval. This is just a suggestion so the implementation might wait longer
   * than the specified duration.
   */
  public Duration suggestedWait() {
    return suggestedWait;
  }

  private static String composedMessage(CostFunction source, String original) {
    return "Not enough metrics for " + source.getClass().getName() + ": " + original;
  }
}
