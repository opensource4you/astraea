package org.astraea.argument;

import com.beust.jcommander.ParameterException;

public class BooleanField implements NonEmptyField<Boolean> {
  @Override
  public Boolean convert(String value) {
    return Boolean.parseBoolean(value);
  }

  @Override
  public void validate(String name, String value) throws ParameterException {
    NonEmptyField.super.validate(name, value);
    if (!("false".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value)))
      throw new ParameterException(value + " is not boolean type");
  }
}
