package org.astraea.app.service;

import java.util.Map;
import java.util.Set;
import org.astraea.app.common.Utils;
import org.junit.jupiter.api.AfterAll;

/**
 * This class offers a way to have embedded kafka cluster. It is useful to test code which is
 * depended on true cluster.
 */
public abstract class RequireBrokerCluster extends RequireJmxServer {
  private static final int NUMBER_OF_BROKERS = 3;
  private static ZookeeperCluster ZOOKEEPER_CLUSTER = Services.zookeeperCluster();
  private static BrokerCluster BROKER_CLUSTER =
      Services.brokerCluster(ZOOKEEPER_CLUSTER, NUMBER_OF_BROKERS);

  protected static String bootstrapServers() {
    return BROKER_CLUSTER.bootstrapServers();
  }

  protected static Map<Integer, Set<String>> logFolders() {
    return BROKER_CLUSTER.logFolders();
  }

  protected static void restartCluster() {
    shutdownClusters();
    ZOOKEEPER_CLUSTER = Services.zookeeperCluster();
    BROKER_CLUSTER = Services.brokerCluster(ZOOKEEPER_CLUSTER, NUMBER_OF_BROKERS);
  }

  protected static void closeBroker(int brokerID) {
    BROKER_CLUSTER.close(brokerID);
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
