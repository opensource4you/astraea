package org.astraea.service;

public interface ZookeeperCluster extends AutoCloseable {

  /** @return zookeeper information. the form is "host_a:port_a,host_b:port_b" */
  String connectionProps();

  /** @return true if this zookeeper cluster is generated locally. */
  boolean isLocal();
}
