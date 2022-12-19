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
package org.astraea.common.metrics.broker;

import java.util.Objects;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.HasBeanObject;

/**
 * You can find some default metric in {@link kafka.metrics.KafkaMetricsGroup}. This object is
 * mapped to {@link com.yammer.metrics.core.Gauge}.Kafka gauges are String, Integer, Long and
 * Double.
 */
public interface HasGauge<T> extends HasBeanObject {
  String VALUE_KEY = "Value";

  static HasGauge<Long> ofLong(BeanObject beanObject) {
    return () -> beanObject;
  }

  @SuppressWarnings("unchecked")
  default T value() {
    return (T) Objects.requireNonNull(beanObject().attributes().get(VALUE_KEY));
  }
}
