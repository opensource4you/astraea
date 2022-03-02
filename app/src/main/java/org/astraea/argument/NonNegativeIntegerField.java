package org.astraea.argument;

public class NonNegativeIntegerField implements PositiveNumberField<Integer> {
  @Override
  public Integer convert(String value) {
    return Integer.parseInt(value);
  }
}
