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

import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import javax.management.ObjectName;
import org.astraea.common.Utils;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.HasBeanObject;

public class JmxProgressReport implements BalancerProgressReport, AutoCloseable {

  private final ObjectName mBeanObjectName;
  private final BalancerExecution balancerExecution;

  public JmxProgressReport(Class<? extends Balancer> balancerClass, String balancerName) {
    this.mBeanObjectName =
        Utils.packException(
            () ->
                new ObjectName(
                    Balancer.class.getPackageName(),
                    new Hashtable<>(
                        Map.of("balancer", balancerClass.getSimpleName(), "name", balancerName))));
    this.balancerExecution = new BalancerExecution();
    Utils.packException(
        () ->
            ManagementFactory.getPlatformMBeanServer()
                .registerMBean(balancerExecution, mBeanObjectName));
  }

  @Override
  public void iteration(long time, double clusterCost) {
    balancerExecution.iterationCount.increment();
    balancerExecution.minScore.accumulate(Double.doubleToRawLongBits(clusterCost));
  }

  @Override
  public void close() {
    Utils.packException(
        () -> ManagementFactory.getPlatformMBeanServer().unregisterMBean(mBeanObjectName));
  }

  public interface BalancerExecutionMBean {
    long getIterationCount();

    double getMinClusterCost();
  }

  private static class BalancerExecution implements BalancerExecutionMBean {

    private final LongAdder iterationCount;
    private final LongAccumulator minScore;

    public BalancerExecution() {
      this.iterationCount = new LongAdder();
      this.minScore =
          new LongAccumulator(this::compareDouble, Double.doubleToRawLongBits(Double.NaN));
    }

    private long compareDouble(long lhs, long rhs) {
      double l = Double.longBitsToDouble(lhs);
      double r = Double.longBitsToDouble(rhs);
      return (Double.isNaN(r) || l < r) ? lhs : rhs;
    }

    @Override
    public long getIterationCount() {
      return iterationCount.sum();
    }

    @Override
    public double getMinClusterCost() {
      return Double.longBitsToDouble(minScore.get());
    }
  }

  public static class BalancerMBean implements HasBeanObject {
    private final BeanObject beanObject;

    public BalancerMBean(BeanObject beanObject) {
      this.beanObject = beanObject;
    }

    public String balancerClass() {
      return beanObject().properties().get("balancer");
    }

    public String name() {
      return beanObject().properties().get("name");
    }

    public long iteration() {
      return (long) beanObject().attributes().get("IterationCount");
    }

    public double minClusterCost() {
      return (double) beanObject().attributes().get("MinClusterCost");
    }

    @Override
    public BeanObject beanObject() {
      return beanObject;
    }
  }
}
