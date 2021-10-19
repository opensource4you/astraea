package org.astraea.service;

import org.astraea.Utils;
import org.junit.jupiter.api.AfterAll;

public class RequireBrokerCluster {
  private static final ZookeeperCluster ZOOKEEPER_CLUSTER = Services.zookeeperCluster();
  private static final BrokerCluster BROKER_CLUSTER = Services.brokerCluster(ZOOKEEPER_CLUSTER, 3);

  public static String bootstrapServers() {
    return BROKER_CLUSTER.bootstrapServers();
  }

  @AfterAll
  static void shutdownClusters() {
    Utils.close(BROKER_CLUSTER);
    Utils.close(ZOOKEEPER_CLUSTER);
  }
}
