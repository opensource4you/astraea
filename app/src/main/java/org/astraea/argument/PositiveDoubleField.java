package org.astraea.argument;

public class PositiveDoubleField extends PositiveNumberField<Double> {
  @Override
  public Double convert(String value) {
    return Double.parseDouble(value);
  }
}
