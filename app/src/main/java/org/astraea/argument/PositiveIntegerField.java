package org.astraea.argument;

public class PositiveIntegerField implements PositiveNumberField<Integer> {
  @Override
  public Integer convert(String value) {
    return Integer.parseInt(value);
  }
}
