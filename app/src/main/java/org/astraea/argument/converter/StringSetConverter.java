package org.astraea.argument.converter;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import java.util.Set;

public class StringSetConverter implements IStringConverter<Set<String>> {
  @Override
  public Set<String> convert(String value) {
    if (value.isEmpty()) throw new ParameterException("array type can't be empty");
    return Set.of(value.split(","));
  }
}
