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
package org.astraea.app.balancer;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.cost.MoveCost;
import org.astraea.app.cost.ReplicaDiskInCost;
import org.astraea.app.partitioner.Configuration;

public class BalancerExperimentMain {

  public static void main(String[] args) {
    /*
    broker.0./tmp/log-folder-0=889000000
    broker.0./tmp/log-folder-1=889000000
    broker.0./tmp/log-folder-2=889000000
    broker.1./tmp/log-folder-0=889000000
    broker.1./tmp/log-folder-1=889000000
    broker.1./tmp/log-folder-2=889000000
    broker.2./tmp/log-folder-0=889000000
    broker.2./tmp/log-folder-1=889000000
    broker.2./tmp/log-folder-2=889000000
    broker.3./tmp/log-folder-0=889000000
    broker.3./tmp/log-folder-1=889000000
    broker.3./tmp/log-folder-2=889000000

             */
    /*
            broker.0=1280
    broker.1=1280
    broker.2=1280
    broker.3=1280
             */
    var config =
        Configuration.of(
            Map.of(
                BalancerConfigs.BOOTSTRAP_SERVERS_CONFIG,
                "192.168.103.177:25655,192.168.103.178:25655,192.168.103.179:25655,192.168.103.180:25655",
                BalancerConfigs.JMX_SERVERS_CONFIG,
                "0@service:jmx:rmi://192.168.103.177:16926/jndi/rmi://192.168.103.177:16926/jmxrmi,1@service:jmx:rmi://192.168.103.178:16926/jndi/rmi://192.168.103.178:16926/jmxrmi,2@service:jmx:rmi://192.168.103.179:16926/jndi/rmi://192.168.103.179:16926/jmxrmi,3@service:jmx:rmi://192.168.103.180:16926/jndi/rmi://192.168.103.180:16926/jmxrmi",
                BalancerConfigs.METRICS_WARM_UP_COUNT_CONFIG,
                "5",
                BalancerConfigs.METRICS_SCRAPING_INTERVAL_MS_CONFIG,
                "10000",
                BalancerConfigs.BALANCER_PLAN_SEARCHING_ITERATION,
                "10000",
                "shuffle.plan.generator.shuffle.min",
                "3",
                "shuffle.plan.generator.shuffle.max",
                "30",
                BalancerConfigs.BALANCER_COST_FUNCTIONS,
                Stream.of(ReplicaDiskInCost.class, MoveCost.class)
                    .map(Class::getName)
                    .collect(Collectors.joining(",")),
                "brokerBandwidthConfig",
                "/home/sean/Documents/kafka_2.13-3.0.0/brokerBandwith.properties",
                "brokerCapacityConfig",
                "/home/sean/Documents/kafka_2.13-3.0.0/brokerCapacity.properties"));

    BalancerMain.execute(config);
  }
}
