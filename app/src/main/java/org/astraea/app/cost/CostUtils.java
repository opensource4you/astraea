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
package org.astraea.app.cost;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.broker.HasCount;
import org.astraea.app.metrics.broker.HasGauge;

public class CostUtils {

  static Map<Integer, Double> brokerTrafficMetrics(
      ClusterBean clusterBean, String metricName, Duration duration) {
    return clusterBean.all().entrySet().stream()
        .map(brokerMetrics -> dataRate(brokerMetrics, metricName, duration))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  static boolean filterBean(HasBeanObject hasBeanObject, String metricName) {
    var beanObject = hasBeanObject.beanObject();
    return beanObject != null
        && beanObject.properties().containsKey("name")
        && beanObject.properties().get("name").equals(metricName);
  }

  static <T> Map.Entry<T, Double> dataRate(
      Map.Entry<T, Collection<HasBeanObject>> brokerMetrics, String metricName, Duration duration) {
    AtomicBoolean brokerLevel = new AtomicBoolean();
    brokerLevel.set(brokerMetrics.getKey() instanceof Integer);

    var sizeTimeSeries =
        brokerMetrics.getValue().stream()
            .filter(
                hasBeanObject -> {
                  if (brokerLevel.get()) return filterBean(hasBeanObject, metricName);
                  return hasBeanObject instanceof HasGauge
                      && hasBeanObject.beanObject().properties().get("type").equals("Log")
                      && hasBeanObject.beanObject().properties().get("name").equals(metricName);
                })
            .sorted(Comparator.comparingLong(HasBeanObject::createdTimestamp).reversed())
            .collect(Collectors.toUnmodifiableList());

    var latestSize = sizeTimeSeries.stream().findFirst().orElseThrow(NoSuchElementException::new);
    var windowSize =
        sizeTimeSeries.stream()
            .dropWhile(
                bean ->
                    bean.createdTimestamp() > latestSize.createdTimestamp() - duration.toMillis())
            .findFirst()
            .orElseThrow();
    var dataRate = -1.0;
    if (brokerLevel.get())
      dataRate =
          (((HasCount) latestSize).count() - ((HasCount) windowSize).count())
              / ((double) (latestSize.createdTimestamp() - windowSize.createdTimestamp()) / 1000)
              / 1024.0
              / 1024.0;
    else {
      dataRate =
          (((HasGauge) latestSize).value() - ((HasGauge) windowSize).value())
              / ((double) (latestSize.createdTimestamp() - windowSize.createdTimestamp()) / 1000)
              / 1024.0
              / 1024.0;
      if (dataRate < 0 || (latestSize == windowSize)) dataRate = -1.0;
    }
    return Map.entry(brokerMetrics.getKey(), dataRate);
  }

  static Map<TopicPartitionReplica, Double> replicaDataRate(
      ClusterBean clusterBean, Duration duration) {
    return clusterBean.mapByReplica().entrySet().parallelStream()
        .map(metrics -> CostUtils.dataRate(metrics, null, duration))
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
