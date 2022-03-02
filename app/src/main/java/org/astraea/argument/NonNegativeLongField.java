package org.astraea.argument;

public class NonNegativeLongField implements PositiveNumberField<Long> {
  @Override
  public Long convert(String value) {
    return Long.parseLong(value);
  }
}
