package org.astraea.argument;

public class NonNegativeDoubleField implements PositiveNumberField<Double> {
  @Override
  public Double convert(String value) {
    return Double.parseDouble(value);
  }
}
