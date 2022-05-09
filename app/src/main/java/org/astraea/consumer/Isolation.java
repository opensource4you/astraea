package org.astraea.consumer;

import java.util.Locale;

public enum Isolation {
  READ_UNCOMMITTED,
  READ_COMMITTED;

  public static Isolation of(String name) {
    return Isolation.valueOf(name.toUpperCase(Locale.ROOT));
  }

  /** @return the name parsed by kafka */
  public String nameOfKafka() {
    return name().toLowerCase(Locale.ROOT);
  }
}
