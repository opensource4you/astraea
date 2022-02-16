package org.astraea.argument.validator;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

public class NotEmptyString implements IParameterValidator {
  @Override
  public void validate(String name, String value) throws ParameterException {
    if (value == null || value.equals(""))
      throw new ParameterException(name + " should not be empty.");
  }
}
