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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
      public Map<TopicPartition, Collection<HasBeanObject>> beanObjectByPartition() {
        return allBeans.entrySet().stream()
            .flatMap(
                entry -> {
                  return entry.getValue().stream()
                      .filter(
                          hasBeanObject ->
                              hasBeanObject.beanObject().getProperties().containsKey("topic")
                                  && hasBeanObject
                                      .beanObject()
                                      .getProperties()
                                      .containsKey("partition"))
                      .map(
                          hasBeanObject -> {
                            var properties = hasBeanObject.beanObject().getProperties();
                            var topic = properties.get("topic");
                            var partition = properties.get("partition");
                            return Map.entry(
                                TopicPartition.of(topic, partition), List.of(hasBeanObject));
                          });
                })
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (x1, x2) -> {
                      var beanObject1 = x1.iterator().next().beanObject();
                      var beanObject2 = x2.iterator().next().beanObject();
                      if (beanObject1.getProperties().equals(beanObject2.getProperties())
                          && beanObject1.domainName().equals(beanObject2.domainName())) return x1;
                      else
                        return Stream.concat(x1.stream(), x2.stream()).collect(Collectors.toList());
                    }));
      }

      @Override
      public Map<TopicPartitionReplica, Collection<HasBeanObject>> beanObjectByReplica() {
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
                                  List.of(hasBeanObject));
                            }))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (x1, x2) -> {
                      var beanObject1 = x1.iterator().next().beanObject();
                      var beanObject2 = x2.iterator().next().beanObject();
                      if (beanObject1.getProperties().equals(beanObject2.getProperties())
                          && beanObject1.domainName().equals(beanObject2.domainName())) return x1;
                      else
                        return Stream.concat(x1.stream(), x2.stream()).collect(Collectors.toList());
                    }));
      }
    };
  }

  /**
   * @return a {@link Map} collection that contains broker as key and Collection of {@link
   *     HasBeanObject} as value.
   */
  Map<Integer, Collection<HasBeanObject>> all();

  /**
   * @return a {@link Map} collection that contains {@link TopicPartition} as key and a {@link
   *     HasBeanObject} as value,note that this can only be used to get partition-related
   *     beanObjects.
   */
  Map<TopicPartition, Collection<HasBeanObject>> beanObjectByPartition();

  /**
   * @return a {@link Map} collection that contains {@link TopicPartitionReplica} as key and a
   *     {@link HasBeanObject} as value,note that this can only be used to get partition-related
   *     beanObjects.
   */
  Map<TopicPartitionReplica, Collection<HasBeanObject>> beanObjectByReplica();
}
