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
package org.astraea.app.web;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.Replica;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.ClusterCost;
import org.astraea.app.cost.HasClusterCost;
import org.astraea.app.cost.HasMoveCost;
import org.astraea.app.cost.MoveCost;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BalancerHandlerTest extends RequireBrokerCluster {

  @Test
  void testReport() {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(10).numberOfReplicas((short) 3).create();
      Utils.sleep(Duration.ofSeconds(3));
      var handler1 = new BalancerHandler(admin)
              .get(Channel.ofQueries(Map.of(BalancerHandler.LIMIT_KEY, "30")));
      var handler = new BalancerHandler(admin, new MyCost(),new MyCost());
      var report =
          Assertions.assertInstanceOf(
              BalancerHandler.Report.class,
              handler.get(Channel.ofQueries(Map.of(BalancerHandler.LIMIT_KEY, "30"))));
      Assertions.assertEquals(30, report.limit);
      Assertions.assertNotEquals(0, report.changes.size());
      Assertions.assertTrue(report.cost >= report.newCost);
      Assertions.assertEquals(MyCost.class.getSimpleName(), report.function);
    }
  }

  private static class MyCost implements HasClusterCost, HasMoveCost {
    private final AtomicInteger count = new AtomicInteger(0);

    @Override
    public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
      var cost = count.getAndIncrement() == 0 ? Double.MAX_VALUE : Math.random() * 100;
      return () -> cost;
    }

    @Override
    public MoveCost moveCost(ClusterInfo<Replica> originClusterInfo, ClusterInfo<Replica> newClusterInfo, ClusterBean clusterBean) {
      var cost = (long) (count.getAndIncrement() == 0 ? Double.MAX_VALUE : Math.random() * 100);
      return new MoveCost() {
        @Override
        public String name() {
          return "size";
        }

        @Override
        public long totalCost() {
          return cost;
        }

        @Override
        public String unit() {
          return "byte";
        }

        @Override
        public Map<Integer, Long> changes() {
          return null;
        }
      };
    }
  }
}
