package org.astraea.metrics.collector;

import java.util.function.Function;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.jmx.MBeanClient;

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
   * @param getter to get metrics from MBeanClient
   * @return this register
   */
  Register metricsGetter(Function<MBeanClient, HasBeanObject> getter);

  /** @return create a Receiver */
  Receiver build();
}
