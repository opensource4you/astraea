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
package org.astraea.app.metrics.broker;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.BeanObject;
import org.astraea.app.metrics.BeanQuery;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.MBeanClient;

public final class LogMetrics {

  public enum Log {
    LOG_END_OFFSET("LogEndOffset"),
    LOG_START_OFFSET("LogStartOffset"),
    NUM_LOG_SEGMENTS("NumLogSegments"),
    SIZE("Size");
    private final String metricName;

    Log(String name) {
      this.metricName = name;
    }

    public String metricName() {
      return metricName;
    }

    public static LogMetrics.Log of(String metricName) {
      return Utils.ofIgnoreCaseEnum(
          LogMetrics.Log.values(), LogMetrics.Log::metricName, metricName);
    }

    public static Collection<Gauge> gauges(Collection<HasBeanObject> beans, Log type) {
      return beans.stream()
          .filter(m -> m instanceof Gauge)
          .map(m -> (Gauge) m)
          .filter(m -> m.type() == type)
          .collect(Collectors.toUnmodifiableList());
    }

    public List<Gauge> fetch(MBeanClient mBeanClient) {
      return mBeanClient
          .queryBeans(
              BeanQuery.builder()
                  .domainName("kafka.log")
                  .property("type", "Log")
                  .property("topic", "*")
                  .property("partition", "*")
                  .property("name", metricName)
                  .build())
          .stream()
          .map(Gauge::new)
          .collect(Collectors.toUnmodifiableList());
    }

    public static class Gauge implements HasGauge {
      private final BeanObject beanObject;

      public Gauge(BeanObject beanObject) {
        this.beanObject = beanObject;
      }

      public String topic() {
        return beanObject().properties().get("topic");
      }

      public int partition() {
        return Integer.parseInt(beanObject().properties().get("partition"));
      }

      public String metricsName() {
        return beanObject().properties().get("name");
      }

      public Log type() {
        return Log.of(metricsName());
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }
    }
  }

  private LogMetrics() {}
}
