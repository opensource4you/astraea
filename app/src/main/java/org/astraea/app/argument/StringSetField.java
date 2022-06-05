package org.astraea.app.argument;

import java.util.Set;

public class StringSetField extends SetField<String> {

  @Override
  public Set<String> convert(String value) {
    return Set.of(value.split(SEPARATOR));
  }
}
