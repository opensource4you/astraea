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
package org.astraea.common.cost;

import java.util.Map;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.broker.LogMetrics;

public class LogMetricsGaugeBuilder {
  private final LogMetrics.Log metric;
  private String topic;
  private int partition;
  private long value;

  public LogMetricsGaugeBuilder(LogMetrics.Log metric) {
    this.metric = metric;
  }

  public LogMetricsGaugeBuilder topic(String topic) {
    this.topic = topic;
    return this;
  }

  public LogMetricsGaugeBuilder partition(int partition) {
    this.partition = partition;
    return this;
  }

  public LogMetricsGaugeBuilder logSize(long value) {
    this.value = value;
    return this;
  }

  public LogMetrics.Log.Gauge build() {
    return new LogMetrics.Log.Gauge(
        new BeanObject(
            LogMetrics.DOMAIN_NAME,
            Map.ofEntries(
                Map.entry("type", LogMetrics.LOG_TYPE),
                Map.entry("name", metric.metricName()),
                Map.entry("topic", topic),
                Map.entry("partition", String.valueOf(partition))),
            Map.of("Value", value)));
  }
}
