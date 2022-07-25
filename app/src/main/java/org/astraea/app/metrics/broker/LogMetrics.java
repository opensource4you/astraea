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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.astraea.app.metrics.jmx.BeanObject;
import org.astraea.app.metrics.jmx.BeanQuery;
import org.astraea.app.metrics.jmx.MBeanClient;

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
      return Arrays.stream(LogMetrics.Log.values())
          .filter(metric -> metric.metricName().equalsIgnoreCase(metricName))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("No such metric: " + metricName));
    }

    public List<Meter> fetch(MBeanClient mBeanClient) {
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
          .map(Meter::new)
          .collect(Collectors.toUnmodifiableList());
    }

    static class Meter implements HasValue {
      private final BeanObject beanObject;

      private Meter(BeanObject beanObject) {
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
