package org.astraea.app.argument;

public class PositiveDoubleField extends PositiveNumberField<Double> {
  @Override
  public Double convert(String value) {
    return Double.parseDouble(value);
  }
}
