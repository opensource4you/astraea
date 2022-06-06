package org.astraea.app.argument;

public class NonEmptyStringField extends Field<String> {
  @Override
  public String convert(String value) {
    return value;
  }
}
