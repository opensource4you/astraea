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
import java.util.Collection;
import java.util.stream.Collectors;
import org.astraea.app.metrics.BeanObject;
import org.astraea.app.metrics.BeanQuery;
import org.astraea.app.metrics.MBeanClient;

public final class ServerMetrics {

  public enum DelayedOperationPurgatory {
    AlterAcls("AlterAcls"),
    DeleteRecords("DeleteRecords"),
    ElectLeader("ElectLeader"),
    Fetch("Fetch"),
    Heartbeat("Heartbeat"),
    Produce("Produce"),
    Rebalance("Rebalance");

    private final String metricName;

    DelayedOperationPurgatory(String name) {
      this.metricName = name;
    }

    public String metricName() {
      return metricName;
    }

    public Collection<Size> fetch(MBeanClient mBeanClient) {
      return mBeanClient
          .queryBeans(
              BeanQuery.builder()
                  .domainName("kafka.server")
                  .property("type", "DelayedOperationPurgatory")
                  .property("delayedOperation", metricName)
                  .property("name", "PurgatorySize")
                  .build())
          .stream()
          .map(Size::new)
          .collect(Collectors.toUnmodifiableList());
    }

    public static DelayedOperationPurgatory of(String metricName) {
      return Arrays.stream(DelayedOperationPurgatory.values())
          .filter(metric -> metric.metricName().equalsIgnoreCase(metricName))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("No such metric: " + metricName));
    }

    public static class Size implements HasValue {
      private final BeanObject beanObject;

      public Size(BeanObject beanObject) {
        this.beanObject = beanObject;
      }

      public String metricsName() {
        return beanObject().properties().get("delayedOperation");
      }

      public DelayedOperationPurgatory type() {
        return DelayedOperationPurgatory.of(metricsName());
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }
    }
  }
}
