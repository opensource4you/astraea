package org.astraea.argument;

public class PositiveShortField implements PositiveNumberField<Short> {
  @Override
  public Short convert(String value) {
    return Short.parseShort(value);
  }
}
