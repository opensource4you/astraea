package org.astraea.app.argument;

public class PositiveIntegerField extends PositiveNumberField<Integer> {
  @Override
  public Integer convert(String value) {
    return Integer.parseInt(value);
  }
}
