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

/**
 * Indicate that {@link CostFunction} thinks the metrics on hand cannot be processed. It probably is
 * due to the insufficient amount of metrics or contradiction in the value of the metric. This
 * exception serves as a hint to the caller that it needs more or newer metrics to function, please
 * retry later.
 */
public class BadMetricsException extends RuntimeException {
  public BadMetricsException() {}

  public BadMetricsException(String message) {
    super(message);
  }

  public BadMetricsException(String message, Throwable cause) {
    super(message, cause);
  }

  public BadMetricsException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
