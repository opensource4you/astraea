package org.astraea.app.metrics.collector;

import java.util.Collection;
import org.astraea.app.metrics.HasBeanObject;

/**
 * This Receiver is used to request mbeans. It must be closed. Otherwise, the true connection will
 * get leaked.
 */
public interface Receiver extends AutoCloseable {

  /** @return host of jmx server */
  String host();

  /** @return port of jmx server */
  int port();

  /**
   * This method may request the latest mbeans if the current mbeans are out-of-date.
   *
   * @return current mbeans.
   */
  Collection<HasBeanObject> current();

  @Override
  void close();
}
