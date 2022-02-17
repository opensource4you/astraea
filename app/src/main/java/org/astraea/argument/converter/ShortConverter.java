package org.astraea.argument.converter;

import com.beust.jcommander.IStringConverter;

public class ShortConverter implements IStringConverter<Short> {
  @Override
  public Short convert(String value) {
    return Short.valueOf(value);
  }
}
