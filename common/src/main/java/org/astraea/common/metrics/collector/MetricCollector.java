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
import java.util.Set;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.metrics.HasBeanObject;

public interface MetricCollector extends AutoCloseable {
  /**
   * @return the current registered sensors.
   */
  Collection<MetricSensor> metricSensors();

  /**
   * @return the current registered identities.
   */
  Set<Integer> listIdentities();

  /**
   * @return the type of metrics has been sampled so far.
   */
  Set<Class<? extends HasBeanObject>> listMetricTypes();

  /**
   * @return size of stored beans
   */
  int size();

  /**
   * @return a weak consistency stream for stored beans.
   */
  Stream<HasBeanObject> metrics();

  /**
   * @return a weak consistency stream for stored beans.
   */
  default <T extends HasBeanObject> Stream<T> metrics(Class<T> clz) {
    return metrics().filter(b -> clz.isAssignableFrom(b.getClass())).map(clz::cast);
  }

  /**
   * @return the {@link ClusterBean}.
   */
  ClusterBean clusterBean();

  @Override
  void close();

  static LocalMetricCollector.Builder local() {
    return new LocalMetricCollector.Builder();
  }

  static InternalTopicCollector.Builder internalTopic() {
    return new InternalTopicCollector.Builder();
  }
}
