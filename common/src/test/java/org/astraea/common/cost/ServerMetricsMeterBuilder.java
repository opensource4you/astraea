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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.broker.ServerMetrics;

public class ServerMetricsMeterBuilder {
  private final ServerMetrics.Topic metric;
  private String topic;
  private long time;
  private final Map<String, Object> attributes = new HashMap<>();

  public ServerMetricsMeterBuilder(ServerMetrics.Topic metric) {
    this.metric = metric;
  }

  public ServerMetricsMeterBuilder topic(String topic) {
    this.topic = topic;
    return this;
  }

  public ServerMetricsMeterBuilder time(long time) {
    this.time = time;
    return this;
  }

  public ServerMetricsMeterBuilder meanRate(double value) {
    this.attributes.put("MeanRate", value);
    return this;
  }

  public ServerMetricsMeterBuilder oneMinuteRate(double value) {
    this.attributes.put("OneMinuteRate", value);
    return this;
  }

  public ServerMetricsMeterBuilder fiveMinuteRate(double value) {
    this.attributes.put("FiveMinuteRate", value);
    return this;
  }

  public ServerMetricsMeterBuilder fifteenMinuteRate(double value) {
    this.attributes.put("FifteenMinuteRate", value);
    return this;
  }

  public ServerMetricsMeterBuilder rateUnit(TimeUnit timeUnit) {
    this.attributes.put("RateUnit", timeUnit);
    return this;
  }

  public ServerMetricsMeterBuilder count(long count) {
    this.attributes.put("Count", count);
    return this;
  }

  public ServerMetrics.Topic.Meter build() {
    return new ServerMetrics.Topic.Meter(
        new BeanObject(
            ServerMetrics.DOMAIN_NAME,
            Map.ofEntries(
                Map.entry("type", "BrokerTopicMetrics"),
                Map.entry("topic", topic),
                Map.entry("name", metric.metricName())),
            Map.copyOf(attributes)));
  }
}
