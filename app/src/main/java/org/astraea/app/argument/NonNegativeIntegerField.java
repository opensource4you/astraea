package org.astraea.app.argument;

public class NonNegativeIntegerField extends NonNegativeNumberField<Integer> {
  @Override
  public Integer convert(String value) {
    return Integer.parseInt(value);
  }
}
