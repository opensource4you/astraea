package org.astraea.argument.converter;

import com.beust.jcommander.IStringConverter;

public class BooleanConverter implements IStringConverter<Boolean> {
  @Override
  public Boolean convert(String value) {
    return Boolean.valueOf(value);
  }
}
