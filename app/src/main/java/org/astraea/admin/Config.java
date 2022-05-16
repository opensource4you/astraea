package org.astraea.admin;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** this interface used to represent the resource (topic or broker) configuration. */
public interface Config extends Iterable<Map.Entry<String, String>> {
  /**
   * @param key config key
   * @return the value associated to input key. otherwise, empty
   */
  Optional<String> value(String key);

  /** @return all keys in this configuration */
  Set<String> keys();

  /** @return all values in this configuration */
  Collection<String> values();
}
