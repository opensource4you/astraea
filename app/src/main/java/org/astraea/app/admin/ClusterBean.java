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
package org.astraea.app.admin;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.app.metrics.HasBeanObject;

/** Used to get beanObject using a variety of different keys . */
public interface ClusterBean {
  static ClusterBean of(Map<Integer, Collection<HasBeanObject>> allBeans) {
    return new ClusterBean() {
      @Override
      public Map<Integer, Collection<HasBeanObject>> all() {
        return Collections.unmodifiableMap(allBeans);
      }

      @Override
      public Map<TopicPartition, HasBeanObject> getBeanObjectByPartition(int brokerId) {
        return allBeans.entrySet().stream()
            .filter(entry -> entry.getKey() == brokerId)
            .flatMap(
                entry ->
                    entry.getValue().stream()
                        .filter(
                            x ->
                                x.beanObject().getProperties().containsKey("topic")
                                    && x.beanObject().getProperties().containsKey("partition"))
                        .map(
                            hasBeanObject -> {
                              var properties = hasBeanObject.beanObject().getProperties();
                              var topic = properties.get("topic");
                              var partition = properties.get("partition");
                              return Map.entry(TopicPartition.of(topic, partition), hasBeanObject);
                            }))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }

      @Override
      public Map<TopicPartitionReplica, HasBeanObject> getBeanObjectByReplica() {
        return allBeans.entrySet().stream()
            .flatMap(
                entry ->
                    entry.getValue().stream()
                        .filter(
                            x ->
                                x.beanObject().getProperties().containsKey("topic")
                                    && x.beanObject().getProperties().containsKey("partition"))
                        .map(
                            hasBeanObject -> {
                              var properties = hasBeanObject.beanObject().getProperties();
                              var topic = properties.get("topic");
                              var partition = Integer.parseInt(properties.get("partition"));
                              return Map.entry(
                                  new TopicPartitionReplica(topic, partition, entry.getKey()),
                                  hasBeanObject);
                            }))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }
    };
  }

  /**
   * @return a {@link Map} collection that contains broker as key and Collection of {@link
   *     HasBeanObject} as value.
   */
  Map<Integer, Collection<HasBeanObject>> all();

  /**
   * @param brokerId broker Id
   * @return a {@link Map} collection that contains {@link TopicPartition} as key and a {@link
   *     HasBeanObject} as value,note that this can only be used to get partition-related
   *     beanObjects.
   */
  Map<TopicPartition, HasBeanObject> getBeanObjectByPartition(int brokerId);

  /**
   * @return a {@link Map} collection that contains {@link TopicPartitionReplica} as key and a
   *     {@link HasBeanObject} as value,note that this can only be used to get partition-related
   *     beanObjects.
   */
  Map<TopicPartitionReplica, HasBeanObject> getBeanObjectByReplica();
}
