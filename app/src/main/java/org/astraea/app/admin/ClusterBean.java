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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.metrics.HasBeanObject;

/** Used to get beanObject using a variety of different keys . */
public interface ClusterBean {
  ClusterBean EMPTY = ClusterBean.of(Map.of());

  private static Collection<HasBeanObject> compareBeanObject(
      Collection<HasBeanObject> x1, Collection<HasBeanObject> x2) {
    var beanObject2 = x2.iterator().next().beanObject();
    if (x1.stream().noneMatch(hasBeanObject -> hasBeanObject.beanObject().equals(beanObject2)))
      return Stream.concat(x1.stream(), x2.stream()).collect(Collectors.toList());
    return x1;
  }

  static ClusterBean of(Map<Integer, Collection<HasBeanObject>> allBeans) {
    var beanObjectByReplica = new HashMap<TopicPartitionReplica, Collection<HasBeanObject>>();
    allBeans.forEach(
        (brokerId, beans) ->
            beans.forEach(
                bean -> {
                  if (bean.beanObject() != null
                      && bean.beanObject().properties().containsKey("topic")
                      && bean.beanObject().properties().containsKey("partition")) {
                    var properties = bean.beanObject().properties();
                    var tpr =
                        TopicPartitionReplica.of(
                            properties.get("topic"),
                            Integer.parseInt(properties.get("partition")),
                            brokerId);
                    beanObjectByReplica
                        .computeIfAbsent(tpr, (ignore) -> new ArrayList<>())
                        .add(bean);
                  }
                }));
    // 61
    return new ClusterBean() {
      @Override
      public Map<Integer, Collection<HasBeanObject>> all() {
        return Collections.unmodifiableMap(allBeans);
      }

      @Override
      public Map<TopicPartitionReplica, Collection<HasBeanObject>> mapByReplica() {
        return beanObjectByReplica;
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
}
