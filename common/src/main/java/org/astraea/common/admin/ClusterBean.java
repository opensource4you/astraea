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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Lazy;
import org.astraea.common.cost.CostFunction;
import org.astraea.common.cost.NoSufficientMetricsException;
import org.astraea.common.metrics.HasBeanObject;

/** Used to get beanObject using a variety of different keys . */
public interface ClusterBean {
  ClusterBean EMPTY = ClusterBean.of(Map.of());

  static ClusterBean of(Map<Integer, Collection<HasBeanObject>> allBeans) {
    var lazyReplica =
        Lazy.of(
            () ->
                allBeans.entrySet().stream()
                    .flatMap(
                        entry ->
                            entry.getValue().stream()
                                .filter(
                                    bean ->
                                        bean.beanObject().properties().containsKey("topic")
                                            && bean.beanObject()
                                                .properties()
                                                .containsKey("partition"))
                                .map(
                                    bean ->
                                        Map.entry(
                                            TopicPartitionReplica.of(
                                                bean.beanObject().properties().get("topic"),
                                                Integer.parseInt(
                                                    bean.beanObject()
                                                        .properties()
                                                        .get("partition")),
                                                entry.getKey()),
                                            bean)))
                    .collect(
                        Collectors.groupingBy(
                            Map.Entry::getKey,
                            Collectors.mapping(
                                Map.Entry::getValue, Collectors.toUnmodifiableList())))
                    .entrySet()
                    .stream()
                    .collect(
                        Collectors.toUnmodifiableMap(
                            Map.Entry::getKey,
                            entry -> (Collection<HasBeanObject>) entry.getValue())));

    return new ClusterBean() {
      @Override
      public Map<Integer, Collection<HasBeanObject>> all() {
        return Collections.unmodifiableMap(allBeans);
      }

      @Override
      public Map<TopicPartitionReplica, Collection<HasBeanObject>> mapByReplica() {
        return lazyReplica.get();
      }
    };
  }

  /**
   * @return a {@link Map} collection that contains broker as key and Collection of {@link
   *     HasBeanObject} as value.
   */
  Map<Integer, Collection<HasBeanObject>> all();

  /**
   * @return a {@link Map} collection that contains {@link TopicPartitionReplica} as key and a
   *     {@link HasBeanObject} as value,note that this can only be used to get partition-related
   *     beanObjects.
   */
  Map<TopicPartitionReplica, Collection<HasBeanObject>> mapByReplica();

  default <T extends HasBeanObject> ClusterBeanQuery<T> select(Class<T> metrics, int from) {
    return new ClusterBeanQuery<>(this::all, metrics, from);
  }

  class ClusterBeanQuery<T extends HasBeanObject> {

    private final Supplier<Map<Integer, Collection<HasBeanObject>>> metricSource;
    private final Class<T> metricType;
    private final int id;
    private final Function<Stream<T>, Stream<T>> typeFilter;
    private Function<Stream<T>, Stream<T>> order = stream -> stream;
    private Function<Stream<T>, Stream<T>> timeWindow = stream -> stream;
    private Function<Stream<T>, Stream<T>> quantities = stream -> stream;
    private final List<Consumer<List<T>>> postCheck = new ArrayList<>();

    private ClusterBeanQuery(
        Supplier<Map<Integer, Collection<HasBeanObject>>> source, Class<T> metricType, int id) {
      this.metricSource = source;
      this.metricType = metricType;
      this.id = id;
      this.typeFilter =
          stream -> stream.filter(bean -> metricType.isAssignableFrom(bean.getClass()));
    }

    /** Sort metrics by time in ascending order. */
    public ClusterBeanQuery<T> ascending() {
      this.order =
          stream -> stream.sorted(Comparator.comparingLong(HasBeanObject::createdTimestamp));
      return this;
    }

    /** Sort metrics by time in descending order. */
    public ClusterBeanQuery<T> descending() {
      this.order =
          stream ->
              stream.sorted(Comparator.comparingLong(HasBeanObject::createdTimestamp).reversed());
      return this;
    }

    /** Retrieve metrics only since specific moment of time. */
    public ClusterBeanQuery<T> metricSince(Duration timeWindow) {
      return metricSince(System.currentTimeMillis() - timeWindow.toMillis());
    }

    public ClusterBeanQuery<T> metricSince(long sinceMs) {
      this.timeWindow = stream -> stream.filter(bean -> sinceMs <= bean.createdTimestamp());
      return this;
    }

    /**
     * Request specific amount of metrics, an {@link NoSufficientMetricsException} will be raised if
     * the requirement doesn't meet.
     */
    public ClusterBeanQuery<T> metricQuantities(int quantity) {
      this.quantities = stream -> stream.limit(quantity);
      this.postCheck.add(
          beans -> {
            if (beans.size() != quantity)
              throw new NoSufficientMetricsException(
                  new CostFunction() {},
                  Duration.ofSeconds(1),
                  "Not enough metrics, expected " + quantity + " but got " + beans.size());
          });
      return this;
    }

    public List<T> run() {
      //noinspection unchecked
      var stream =
          (Stream<T>)
              Objects.requireNonNull(metricSource.get().get(id), "No such identity: " + id)
                  .stream();
      stream = typeFilter.apply(stream);
      stream = timeWindow.apply(stream);
      stream = order.apply(stream);
      stream = quantities.apply(stream);

      var result = stream.collect(Collectors.toUnmodifiableList());
      postCheck.forEach(check -> check.accept(result));
      return result;
    }
  }
}
