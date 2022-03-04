package org.astraea.argument;

public class NonNegativeShortField extends NonNegativeNumberField<Short> {
  @Override
  public Short convert(String value) {
    return Short.parseShort(value);
  }
}
