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
package org.astraea.common.admin;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import org.astraea.common.cost.NoSufficientMetricsException;
import org.astraea.common.metrics.HasBeanObject;

/**
 * Describe an operation that transforms given metrics into the desired result.
 *
 * @param <Result> the query result type.
 */
public interface ClusterBeanQuery<Result> {

  /**
   * Select a window of metrics.
   *
   * @param metricClass the metric type to query. Due to Java language design, it doesn't support
   *     the querying of anonymous class
   * @param id the identity number of the metric source.
   */
  static <T extends HasBeanObject> WindowQuery<T> window(Class<T> metricClass, int id) {
    return new WindowQuery<>(metricClass, id);
  }

  /**
   * Select the latest metric.
   *
   * @param metricClass the metric type to query. Due to Java language design, it doesn't support
   *     the querying of anonymous class
   * @param id the identity number of the metric source.
   */
  static <T extends HasBeanObject> LatestMetricQuery<T> latest(Class<T> metricClass, int id) {
    return new LatestMetricQuery<>(metricClass, id);
  }

  class WindowQuery<T extends HasBeanObject> implements ClusterBeanQuery<List<T>> {

    final Class<T> metricType;
    final int id;
    final Comparator<T> comparator;
    final Predicate<T> filter;
    final int requiredSize;

    private WindowQuery(Class<T> metricType, int id) {
      this.metricType = metricType;
      this.id = id;
      this.comparator = Comparator.comparingInt(bean -> 0);
      this.filter = metric -> true;
      this.requiredSize = -1;
    }

    private WindowQuery(
        Class<T> metricType,
        int id,
        Comparator<T> comparator,
        Predicate<T> filter,
        int requiredSize) {
      this.metricType = metricType;
      this.id = id;
      this.comparator = comparator;
      this.filter = filter;
      this.requiredSize = requiredSize;
    }

    /** Sort metrics by time in ascending order. */
    public WindowQuery<T> ascending() {
      return new WindowQuery<>(
          metricType, id, Comparator.comparingLong(T::createdTimestamp), filter, requiredSize);
    }

    /** Sort metrics by time in descending order. */
    public WindowQuery<T> descending() {
      return new WindowQuery<>(
          metricType,
          id,
          Comparator.comparingLong(T::createdTimestamp).reversed(),
          filter,
          requiredSize);
    }

    /** Retrieve metrics only since specific moment of time. */
    public WindowQuery<T> metricSince(Duration timeWindow) {
      return metricSince(System.currentTimeMillis() - timeWindow.toMillis());
    }

    public WindowQuery<T> metricSince(long sinceMs) {
      return new WindowQuery<>(
          metricType, id, comparator, (bean -> sinceMs <= bean.createdTimestamp()), requiredSize);
    }

    /**
     * Request specific amount of metrics, an {@link NoSufficientMetricsException} will be raised if
     * the requirement doesn't meet.
     */
    public WindowQuery<T> metricQuantities(int quantity) {
      return new WindowQuery<>(metricType, id, comparator, filter, quantity);
    }
  }

  class LatestMetricQuery<T extends HasBeanObject> implements ClusterBeanQuery<T> {

    final Class<T> metricClass;
    final int id;

    private LatestMetricQuery(Class<T> metricClass, int id) {
      this.metricClass = metricClass;
      this.id = id;
    }
  }
}
