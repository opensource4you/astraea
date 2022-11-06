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
package org.astraea.common.balancer.reports;

import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.Supplier;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JmxProgressReportTest {

  @Test
  void test() {
    var balancerClass = GreedyBalancer.class.getSimpleName();
    var balancerId = UUID.randomUUID().toString();
    try (var client = MBeanClient.local()) {
      Supplier<JmxProgressReport.BalancerMBean> balancerMbean =
          () ->
              new JmxProgressReport.BalancerMBean(
                  client.queryBean(
                      BeanQuery.builder()
                          .domainName(Balancer.class.getPackageName())
                          .property("balancer", balancerClass)
                          .property("name", balancerId)
                          .build()));
      try (var report = new JmxProgressReport(GreedyBalancer.class, balancerId)) {

        // initial value
        var bean0 = balancerMbean.get();
        System.out.println(bean0.beanObject());
        Assertions.assertEquals(
            balancerClass, bean0.balancerClass(), bean0.beanObject().toString());
        Assertions.assertEquals(balancerId, bean0.name(), bean0.beanObject().toString());
        Assertions.assertEquals(0, bean0.iteration(), bean0.beanObject().toString());
        Assertions.assertEquals(Double.NaN, bean0.minClusterCost(), bean0.beanObject().toString());

        // update 1
        report.iteration(System.currentTimeMillis(), 1.0);
        var bean1 = balancerMbean.get();
        System.out.println(bean1.beanObject());
        Assertions.assertEquals(1, bean1.iteration(), bean0.beanObject().toString());
        Assertions.assertEquals(1.0, bean1.minClusterCost(), bean0.beanObject().toString());

        // update 2
        report.iteration(System.currentTimeMillis(), 0.7);
        report.iteration(System.currentTimeMillis(), 0.8);
        report.iteration(System.currentTimeMillis(), 0.6);
        report.iteration(System.currentTimeMillis(), 0.9);
        var bean2 = balancerMbean.get();
        System.out.println(bean2.beanObject());
        Assertions.assertEquals(5, bean2.iteration(), bean0.beanObject().toString());
        Assertions.assertEquals(0.6, bean2.minClusterCost(), bean0.beanObject().toString());
      }

      // MBean unregistered
      Assertions.assertThrows(
          NoSuchElementException.class, balancerMbean::get, "The MBean has been unregistered");
    }
  }
}
