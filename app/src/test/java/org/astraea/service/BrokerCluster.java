package org.astraea.service;

import java.util.Map;
import java.util.Set;

public interface BrokerCluster extends AutoCloseable {

  /** @return brokers information. the form is "host_a:port_a,host_b:port_b" */
  String bootstrapServers();

  /** @return true if this broker cluster is generated locally. */
  boolean isLocal();

  /** @return the log folders used by each broker */
  Map<Integer, Set<String>> logFolders();
}
