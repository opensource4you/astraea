package org.astraea.argument;

public class NonEmptyStringField extends Field<String> {
  @Override
  public String convert(String value) {
    return value;
  }
}
