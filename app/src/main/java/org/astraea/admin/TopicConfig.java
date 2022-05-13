package org.astraea.admin;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface TopicConfig extends Iterable<Map.Entry<String, String>> {

  Optional<String> value(String key);

  Set<String> keys();

  Collection<String> values();
}
