package org.astraea.argument;

public class NonNegativeDoubleField extends NonNegativeNumberField<Double> {
  @Override
  public Double convert(String value) {
    return Double.parseDouble(value);
  }
}
