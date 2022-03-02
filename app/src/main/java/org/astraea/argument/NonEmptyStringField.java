package org.astraea.argument;

public class NonEmptyStringField implements NonEmptyField<String> {
  @Override
  public String convert(String value) {
    return value;
  }
}
