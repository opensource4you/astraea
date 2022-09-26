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
package org.astraea.it;

import java.net.URL;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterAll;

/**
 * This class offers a way to have 3 node embedded kafka worker. It is useful to test code which is
 * depended on true cluster.
 */
public abstract class RequireWorkerCluster extends RequireJmxServer {
  private static final int NUMBER_OF_BROKERS = 3;
  private static final ZookeeperCluster ZOOKEEPER_CLUSTER = Services.zookeeperCluster();
  private static final BrokerCluster BROKER_CLUSTER =
      Services.brokerCluster(ZOOKEEPER_CLUSTER, NUMBER_OF_BROKERS);

  private static final WorkerCluster WORKER_CLUSTER =
      Services.workerCluster(BROKER_CLUSTER, new int[] {0, 0, 0});

  protected static String bootstrapServers() {
    return BROKER_CLUSTER.bootstrapServers();
  }

  protected static List<URL> workerUrls() {
    return WORKER_CLUSTER.workerUrls();
  }

  /** @return url of any worker */
  protected static URL workerUrl() {
    var urls = WORKER_CLUSTER.workerUrls();
    int randomNum = ThreadLocalRandom.current().nextInt(0, urls.size());
    return urls.get(randomNum);
  }

  @AfterAll
  static void shutdownClusters() throws Exception {
    WORKER_CLUSTER.close();
    BROKER_CLUSTER.close();
    ZOOKEEPER_CLUSTER.close();
  }
}
