package org.astraea.service;

import org.astraea.Utils;
import org.junit.jupiter.api.AfterAll;

/**
 * This class offers a way to have embedded kafka cluster. It is useful to test code which is
 * depended on true cluster.
 */
public abstract class RequireBrokerCluster {
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
