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
package org.astraea.app.service;

import java.util.Map;
import java.util.Set;
import org.astraea.app.common.Utils;
import org.junit.jupiter.api.AfterAll;

/**
 * This class offers a way to have single node embedded kafka cluster. It is useful to test code which is
 * depended on true cluster.
 */
public abstract class RequireBrokerCluster extends RequireJmxServer {
  private static final int NUMBER_OF_BROKERS = 3;
  private static final ZookeeperCluster ZOOKEEPER_CLUSTER = Services.zookeeperCluster();
  private static final BrokerCluster BROKER_CLUSTER =
      Services.brokerCluster(ZOOKEEPER_CLUSTER, NUMBER_OF_BROKERS);

  protected static String bootstrapServers() {
    return BROKER_CLUSTER.bootstrapServers();
  }

  protected static Map<Integer, Set<String>> logFolders() {
    return BROKER_CLUSTER.logFolders();
  }

  protected static void closeBroker(int brokerIndex) {
    BROKER_CLUSTER.close(brokerIndex);
  }

  protected static Set<Integer> brokerIds() {
    return logFolders().keySet();
  }

  @AfterAll
  static void shutdownClusters() {
    Utils.swallowException(BROKER_CLUSTER::close);
    Utils.swallowException(ZOOKEEPER_CLUSTER::close);
  }
}
