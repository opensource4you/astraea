package org.astraea.service;

import java.util.Map;
import java.util.Set;

public interface BrokerCluster extends AutoCloseable {

  /** @return brokers information. the form is "host_a:port_a,host_b:port_b" */
  String bootstrapServers();

  /** @return the log folders used by each broker */
  Map<Integer, Set<String>> logFolders();

  /** @param the broker id want to close */
  void close(int brokerID);
}
