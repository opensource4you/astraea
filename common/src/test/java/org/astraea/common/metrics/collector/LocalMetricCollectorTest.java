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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.astraea.common.Configuration;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.it.Service;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class LocalMetricCollectorTest {
  Service SERVICE = Service.builder().numberOfBrokers(2).numberOfWorkers(0).build();

  @Test
  void testReconfigureJmx() {
    var opened = new AtomicInteger(0);
    try (var mock =
        Mockito.mockStatic(
            MBeanClient.class,
            invocation -> {
              if (invocation.getMethod().getName().equals("jndi")) {
                opened.incrementAndGet();
                return new MBeanClient() {
                  @Override
                  public BeanObject bean(BeanQuery beanQuery) {
                    return null;
                  }

                  @Override
                  public Collection<BeanObject> beans(BeanQuery beanQuery) {
                    return null;
                  }

                  @Override
                  public void close() {
                    opened.decrementAndGet();
                  }
                };
              } else {
                return invocation.callRealMethod();
              }
            })) {
      var clients =
          Map.of(
              1,
              MBeanClient.jndi(
                  SERVICE.jmxServiceURL().getHost(), SERVICE.jmxServiceURL().getPort()),
              -1,
              MBeanClient.local());
      Assertions.assertEquals(1, opened.get());
      try (var collector =
          new LocalMetricCollector(
              1, Duration.ZERO, Duration.ofSeconds(1), Duration.ofMinutes(1), clients, List.of())) {
        // Test target creation
        Assertions.assertEquals(Set.of(-1, 1), collector.listIdentities());
        Assertions.assertEquals(1, opened.get());
        // Test target deletion
        collector.reconfigure(Configuration.of(Map.of()));
        Assertions.assertEquals(Set.of(-1), collector.listIdentities());
        Assertions.assertEquals(0, opened.get());
        // Test target addition
        collector.reconfigure(
            Configuration.of(
                Map.of(
                    "registerJmx.2",
                    SERVICE.jmxServiceURL().getHost() + ":" + SERVICE.jmxServiceURL().getPort())));
        Assertions.assertEquals(Set.of(-1, 2), collector.listIdentities());
        Assertions.assertEquals(1, opened.get());
      }
      // Test all jndi clients are closed
      Assertions.assertEquals(0, opened.get());
    }
  }
}
