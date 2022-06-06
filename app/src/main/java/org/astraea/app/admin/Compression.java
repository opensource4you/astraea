package org.astraea.app.admin;

import java.util.Locale;

public enum Compression {
  NONE,
  GZIP,
  SNAPPY,
  LZ4,
  ZSTD;

  public static Compression of(String name) {
    return Compression.valueOf(name.toUpperCase(Locale.ROOT));
  }

  /** @return the name parsed by kafka */
  public String nameOfKafka() {
    return name().toLowerCase(Locale.ROOT);
  }
}
