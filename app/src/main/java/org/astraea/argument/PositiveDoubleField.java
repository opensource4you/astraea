package org.astraea.argument;

public class PositiveDoubleField implements PositiveNumberField<Double> {
  @Override
  public Double convert(String value) {
    return Double.parseDouble(value);
  }
}
