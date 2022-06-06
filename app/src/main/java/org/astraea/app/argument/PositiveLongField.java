package org.astraea.app.argument;

public class PositiveLongField extends PositiveNumberField<Long> {
  @Override
  public Long convert(String value) {
    return Long.parseLong(value);
  }
}
