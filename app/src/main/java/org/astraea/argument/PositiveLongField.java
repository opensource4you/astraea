package org.astraea.argument;

public class PositiveLongField implements PositiveNumberField<Long> {
  @Override
  public Long convert(String value) {
    return Long.parseLong(value);
  }
}
