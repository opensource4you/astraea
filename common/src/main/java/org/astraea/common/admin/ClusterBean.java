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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Lazy;
import org.astraea.common.metrics.HasBeanObject;

/** Used to get beanObject using a variety of different keys . */
public interface ClusterBean {
  ClusterBean EMPTY = ClusterBean.of(Map.of());

  static ClusterBean of(Map<Integer, Collection<HasBeanObject>> allBeans) {
    return new ClusterBean() {
      final Map<Class<? extends HasBeanObject>, Lazy<Map<?, ?>>> topicCache =
          new ConcurrentHashMap<>();
      final Map<Class<? extends HasBeanObject>, Lazy<Map<?, ?>>> topicPartitionCache =
          new ConcurrentHashMap<>();
      final Map<Class<? extends HasBeanObject>, Lazy<Map<?, ?>>> topicPartitionReplicaCache =
          new ConcurrentHashMap<>();
      final Map<Class<? extends HasBeanObject>, Lazy<Map<?, ?>>> brokerTopicCache =
          new ConcurrentHashMap<>();

      @Override
      public Map<Integer, Collection<HasBeanObject>> all() {
        return Collections.unmodifiableMap(allBeans);
      }

      @SuppressWarnings("unchecked")
      @Override
      public <Bean extends HasBeanObject> Stream<Bean> topicMetrics(
          String topic, Class<Bean> metricClass) {
        return ((Map<String, List<Bean>>)
                topicCache
                    .computeIfAbsent(
                        metricClass, (m) -> Lazy.of(() -> map(m, (id, bean) -> bean.topicIndex())))
                    .get())
            .getOrDefault(topic, List.of()).stream();
      }

      @SuppressWarnings("unchecked")
      @Override
      public <Bean extends HasBeanObject> Stream<Bean> partitionMetrics(
          TopicPartition topicPartition, Class<Bean> metricClass) {
        return ((Map<TopicPartition, List<Bean>>)
                topicPartitionCache
                    .computeIfAbsent(
                        metricClass,
                        (m) -> Lazy.of(() -> map(m, (id, bean) -> bean.partitionIndex())))
                    .get())
            .getOrDefault(topicPartition, List.of()).stream();
      }

      @SuppressWarnings("unchecked")
      @Override
      public <Bean extends HasBeanObject> Stream<Bean> replicaMetrics(
          TopicPartitionReplica replica, Class<Bean> metricClass) {
        return ((Map<TopicPartitionReplica, List<Bean>>)
                topicPartitionReplicaCache
                    .computeIfAbsent(
                        metricClass,
                        (m) -> Lazy.of(() -> map(m, (id, bean) -> bean.replicaIndex(id))))
                    .get())
            .getOrDefault(replica, List.of()).stream();
      }

      @SuppressWarnings("unchecked")
      @Override
      public <Bean extends HasBeanObject> Stream<Bean> brokerTopicMetrics(
          BrokerTopic brokerTopic, Class<Bean> metricClass) {
        return ((Map<BrokerTopic, List<Bean>>)
                brokerTopicCache
                    .computeIfAbsent(
                        metricClass,
                        (m) -> Lazy.of(() -> map(m, (id, bean) -> bean.brokerTopicIndex(id))))
                    .get())
            .getOrDefault(brokerTopic, List.of()).stream();
      }

      private <Bean extends HasBeanObject, Key> Map<Key, List<Bean>> map(
          Class<Bean> metricClass, BiFunction<Integer, Bean, Optional<Key>> keyMapper) {
        return all().entrySet().stream()
            .flatMap(
                e ->
                    e.getValue().stream()
                        .filter(bean -> bean.getClass() == metricClass)
                        .map(metricClass::cast)
                        .flatMap(
                            bean ->
                                keyMapper
                                    .apply(e.getKey(), bean)
                                    .map(key -> Map.entry(key, bean))
                                    .stream()))
            .collect(
                Collectors.groupingBy(
                    Map.Entry::getKey,
                    Collectors.mapping(Map.Entry::getValue, Collectors.toUnmodifiableList())));
      }
    };
  }

  /**
   * @return a {@link Map} collection that contains broker as key and Collection of {@link
   *     HasBeanObject} as value.
   */
  Map<Integer, Collection<HasBeanObject>> all();

  /**
   * Query a specific class of metric where they are from the specified topic.
   *
   * @param topic to query.
   * @param metricClass to query.
   * @return a stream of the matched metrics.
   * @param <Bean> the metric to query.
   */
  <Bean extends HasBeanObject> Stream<Bean> topicMetrics(String topic, Class<Bean> metricClass);

  /**
   * Query a specific class of metric where they are from the specified partition.
   *
   * @param topicPartition to query.
   * @param metricClass to query.
   * @return a stream of the matched metrics.
   * @param <Bean> the metric to query.
   */
  <Bean extends HasBeanObject> Stream<Bean> partitionMetrics(
      TopicPartition topicPartition, Class<Bean> metricClass);

  /**
   * Query a specific class of metric where they are from the specified replica.
   *
   * @param replica to query.
   * @param metricClass to query.
   * @return a stream of the matched metrics.
   * @param <Bean> the metric to query.
   */
  <Bean extends HasBeanObject> Stream<Bean> replicaMetrics(
      TopicPartitionReplica replica, Class<Bean> metricClass);

  /**
   * Query a specific class of metric where they are sampled from the specific broker and are
   * related to a specific topic.
   *
   * @param brokerTopic to query.
   * @param metricClass to query.
   * @return a stream of the matched metrics.
   * @param <Bean> the metric to query.
   */
  <Bean extends HasBeanObject> Stream<Bean> brokerTopicMetrics(
      BrokerTopic brokerTopic, Class<Bean> metricClass);

  /**
   * @return the set of topic that has some related metrics within the internal storage.
   */
  default Set<String> topics() {
    return all().values().stream()
        .flatMap(bs -> bs.stream().flatMap(b -> b.topicIndex().stream()))
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * @return the set of partition that has some related metrics within the internal storage.
   */
  default Set<TopicPartition> partitions() {
    return all().values().stream()
        .flatMap(bs -> bs.stream().flatMap(b -> b.partitionIndex().stream()))
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * @return the set of replicas that has some related metrics within the internal storage.
   */
  default Set<TopicPartitionReplica> replicas() {
    return all().entrySet().stream()
        .flatMap(e -> e.getValue().stream().flatMap(b -> b.replicaIndex(e.getKey()).stream()))
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * @return the set of broker/topic pair that has some related metrics within the internal storage.
   */
  default Set<BrokerTopic> brokerTopics() {
    return all().entrySet().stream()
        .flatMap(e -> e.getValue().stream().flatMap(b -> b.brokerTopicIndex(e.getKey()).stream()))
        .collect(Collectors.toUnmodifiableSet());
  }
}
