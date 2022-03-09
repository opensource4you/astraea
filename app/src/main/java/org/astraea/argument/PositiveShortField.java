package org.astraea.argument;

public class PositiveShortField extends PositiveNumberField<Short> {
  @Override
  public Short convert(String value) {
    return Short.parseShort(value);
  }
}
