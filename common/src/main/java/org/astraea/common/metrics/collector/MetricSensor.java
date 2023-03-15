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
package org.astraea.common.metrics.collector;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;

@FunctionalInterface
public interface MetricSensor {
  /**
   * merge all sensors into single one.
   *
   * @param metricSensors cost function
   * @return sensor if there is available sensor. Otherwise, empty is returned
   */
  static Optional<MetricSensor> of(Collection<MetricSensor> metricSensors) {
    if (metricSensors.isEmpty()) return Optional.empty();
    return of(metricSensors, (ex) -> {});
  }

  /**
   * merge all sensors and their exception handler into single one
   *
   * @param metricSensors cost function and exception handler
   * @return sensor if there is available sensor. Otherwise, empty is returned
   */
  static Optional<MetricSensor> of(
      Collection<MetricSensor> metricSensors, Consumer<Exception> exceptionHandler) {
    if (metricSensors.isEmpty()) return Optional.empty();
    return Optional.of(
        (client, clusterBean) ->
            metricSensors.stream()
                .flatMap(
                    ms -> {
                      try {
                        return ms.fetch(client, clusterBean).stream();
                      } catch (NoSuchElementException ex) {
                        exceptionHandler.accept(ex);
                        return Stream.empty();
                      }
                    })
                .collect(Collectors.toUnmodifiableList()));
  }
  /**
   * generate the metrics to stored by metrics collector. The implementation can use MBeanClient to
   * fetch metrics from remote/local mbean server. Or the implementation can generate custom metrics
   * according to existent cluster bean
   *
   * @param client mbean client (don't close it!)
   * @param bean current cluster bean
   * @return java metrics
   */
  Collection<? extends HasBeanObject> fetch(MBeanClient client, ClusterBean bean);
}
