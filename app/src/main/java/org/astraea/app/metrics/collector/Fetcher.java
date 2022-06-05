package org.astraea.app.metrics.collector;

import java.util.Collection;
import java.util.stream.Collectors;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.jmx.MBeanClient;

@FunctionalInterface
public interface Fetcher {

  /**
   * merge multiples fetchers to single one.
   *
   * @param fs fetchers
   * @return single fetcher
   */
  static Fetcher of(Collection<Fetcher> fs) {
    return client ->
        fs.stream().flatMap(f -> f.fetch(client).stream()).collect(Collectors.toUnmodifiableList());
  }

  /**
   * fetch to specify metrics from remote JMX server
   *
   * @param client mbean client (don't close it!)
   * @return java metrics
   */
  Collection<HasBeanObject> fetch(MBeanClient client);
}
