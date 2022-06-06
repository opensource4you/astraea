package org.astraea.app.argument;

public class NonNegativeLongField extends NonNegativeNumberField<Long> {
  @Override
  public Long convert(String value) {
    return Long.parseLong(value);
  }
}
