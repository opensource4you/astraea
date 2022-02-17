package org.astraea.argument.converter;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IntegerSetConverter implements IStringConverter<Set<Integer>> {
  @Override
  public Set<Integer> convert(String value) {
    if (value.isEmpty()) throw new ParameterException("array type can't be empty");
    return Stream.of(value.split(",")).map(Integer::valueOf).collect(Collectors.toSet());
  }
}
