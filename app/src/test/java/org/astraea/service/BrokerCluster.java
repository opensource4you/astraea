package org.astraea.service;

public interface BrokerCluster extends AutoCloseable {

  /** @return brokers information. the form is "host_a:port_a,host_b:port_b" */
  String bootstrapServers();

  /** @return true if this broker cluster is generated locally. */
  boolean isLocal();
}
