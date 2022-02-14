package org.astraea.argument.validator;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

public class NonNegativeLong implements IParameterValidator {
  @Override
  public void validate(String name, String value) throws ParameterException {
    if (Long.parseLong(value) < 0) throw new ParameterException(name + " should not be negative.");
  }
}
