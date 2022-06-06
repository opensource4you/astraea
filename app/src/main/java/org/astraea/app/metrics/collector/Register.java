package org.astraea.app.metrics.collector;

public interface Register {

  /**
   * @param host of jmx server
   * @return this register
   */
  Register host(String host);

  /**
   * @param port of jmx server
   * @return this register
   */
  Register port(int port);

  /**
   * @param getters to get metrics from MBeanClient
   * @return this register
   */
  Register fetcher(Fetcher fetcher);

  /** @return create a Receiver */
  Receiver build();
}
