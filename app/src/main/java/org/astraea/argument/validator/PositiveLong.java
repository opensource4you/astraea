package org.astraea.argument.validator;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

public class PositiveLong implements IParameterValidator {
  @Override
  public void validate(String name, String value) throws ParameterException {
    if (Long.parseLong(value) <= 0) throw new ParameterException(name + " should be positive.");
  }
}
