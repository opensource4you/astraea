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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.astraea.common.cost.CostFunction;
import org.astraea.common.cost.NoSufficientMetricsException;
import org.astraea.common.metrics.HasBeanObject;

/**
 * Describe an operation that transforms given metrics into the desired result.
 *
 * @param <Result> the query result type.
 */
public interface ClusterBeanQuery<Result> {

  /** Execute the query. */
  Result run(Map<Integer, Collection<HasBeanObject>> allMetrics);

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

    private final Class<T> metricType;
    private final int id;
    private final Comparator<T> comparator;
    private final Predicate<T> filter;
    private final int requiredSize;

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

    @Override
    public List<T> run(Map<Integer, Collection<HasBeanObject>> allMetrics) {
      var result =
          Objects.requireNonNull(allMetrics.get(id), "No such identity: " + id).stream()
              .filter(bean -> metricType == bean.getClass())
              .map(metricType::cast)
              .filter(filter)
              .sorted(comparator)
              .limit(requiredSize)
              .collect(Collectors.toUnmodifiableList());
      if (result.size() != requiredSize)
        throw new NoSufficientMetricsException(
            new CostFunction() {},
            Duration.ofSeconds(1),
            "Not enough metrics, expected " + requiredSize + " but got " + result.size());
      return result;
    }
  }

  class LatestMetricQuery<T extends HasBeanObject> implements ClusterBeanQuery<T> {

    private final Class<T> metricClass;
    private final int id;

    private LatestMetricQuery(Class<T> metricClass, int id) {
      this.metricClass = metricClass;
      this.id = id;
    }

    @Override
    public T run(Map<Integer, Collection<HasBeanObject>> allMetrics) {
      return Objects.requireNonNull(allMetrics.get(id), "No such id: " + id).stream()
          .filter(bean -> bean.getClass() == metricClass)
          .max(Comparator.comparingLong(HasBeanObject::createdTimestamp))
          .map(metricClass::cast)
          .orElseThrow(
              () -> new NoSufficientMetricsException(new CostFunction() {}, Duration.ofSeconds(1)));
    }
  }
}
