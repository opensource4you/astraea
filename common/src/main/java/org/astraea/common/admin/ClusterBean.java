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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.Sensor;

/** Used to get beanObject using a variety of different keys . */
public interface ClusterBean {
  ClusterBean EMPTY = ClusterBean.of(Map.of(), Map.of());

  static ClusterBean of(Map<Integer, Collection<HasBeanObject>> allBeans) {
    return of(allBeans, Map.of());
  }

  static ClusterBean of(
      Map<Integer, Collection<HasBeanObject>> allBeans,
      Map<String, Map<?, Sensor<Double>>> sensors) {
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
    return new ClusterBean() {
      @Override
      public Map<Integer, Collection<HasBeanObject>> all() {
        return Collections.unmodifiableMap(allBeans);
      }

      @Override
      public Map<TopicPartitionReplica, Collection<HasBeanObject>> mapByReplica() {
        return beanObjectByReplica;
      }

      @Override
      public Map<TopicPartitionReplica, Double> statisticsByReplica(
          String metricsName, String statName) {
        return sensors.get(metricsName).entrySet().stream()
            .filter(sensor -> sensor.getKey() instanceof TopicPartitionReplica)
            .map(
                sensor ->
                    Map.entry(
                        (TopicPartitionReplica) sensor.getKey(),
                        sensor.getValue().metrics().get(statName).measure()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }

      @Override
      public Map<Integer, Double> statisticsByNode(String metricsName, String statName) {
        return sensors.getOrDefault(metricsName, Map.of()).entrySet().stream()
            .filter(sensor -> sensor.getKey() instanceof Integer)
            .map(
                sensor ->
                    Map.entry(
                        (int) sensor.getKey(), sensor.getValue().metrics().get(statName).measure()))
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
   * @return a {@link Map} collection that contains {@link TopicPartitionReplica} as key and a
   *     {@link HasBeanObject} as value,note that this can only be used to get partition-related
   *     beanObjects.
   */
  Map<TopicPartitionReplica, Collection<HasBeanObject>> mapByReplica();

  /**
   * @param metricsName the metricName to be statistic
   * @param statName select the {@link org.astraea.common.metrics.stats.Stat} to used
   * @return the statistical values corresponding to all replicas
   */
  Map<TopicPartitionReplica, Double> statisticsByReplica(String metricsName, String statName);

  /**
   * @param metricsName the metricName to be statistic
   * @param statName select the {@link org.astraea.common.metrics.stats.Stat} to used
   * @return the statistical values corresponding to all brokers
   */
  Map<Integer, Double> statisticsByNode(String metricsName, String statName);
}
