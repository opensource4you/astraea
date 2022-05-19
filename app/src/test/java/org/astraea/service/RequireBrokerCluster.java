package org.astraea.service;

import java.util.Map;
import java.util.Set;
import org.astraea.Utils;
import org.junit.jupiter.api.AfterAll;

/**
 * This class offers a way to have embedded kafka cluster. It is useful to test code which is
 * depended on true cluster.
 */
public abstract class RequireBrokerCluster extends RequireJmxServer {
  private static final ZookeeperCluster ZOOKEEPER_CLUSTER = Services.zookeeperCluster();
  private static final BrokerCluster BROKER_CLUSTER = Services.brokerCluster(ZOOKEEPER_CLUSTER, 3);

  protected static String bootstrapServers() {
    return BROKER_CLUSTER.bootstrapServers();
  }

  protected static Map<Integer, Set<String>> logFolders() {
    return BROKER_CLUSTER.logFolders();
  }

  protected static void closeBroker(int brokerID) {
    BROKER_CLUSTER.close(brokerID);
  }

  protected static Set<Integer> brokerIds() {
    return logFolders().keySet();
  }

  @AfterAll
  static void shutdownClusters() {
    Utils.close(BROKER_CLUSTER);
    Utils.close(ZOOKEEPER_CLUSTER);
  }
}
