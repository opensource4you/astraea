package org.astraea.app.argument;

public class NonNegativeDoubleField extends NonNegativeNumberField<Double> {
  @Override
  public Double convert(String value) {
    return Double.parseDouble(value);
  }
}
