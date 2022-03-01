package org.astraea.argument;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IntegerSetField implements SetField<Integer> {
  @Override
  public Set<Integer> convert(String value) {
    return Stream.of(value.split(SEPARATOR)).map(Integer::valueOf).collect(Collectors.toSet());
  }
}
