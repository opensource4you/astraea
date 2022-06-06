package org.astraea.app.argument;

import com.beust.jcommander.ParameterException;

public class BooleanField extends Field<Boolean> {
  @Override
  public Boolean convert(String value) {
    return Boolean.parseBoolean(value);
  }

  @Override
  protected void check(String name, String value) throws ParameterException {
    if (!("false".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value)))
      throw new ParameterException(value + " is not boolean type");
  }
}
