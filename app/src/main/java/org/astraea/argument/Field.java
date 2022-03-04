package org.astraea.argument;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

public abstract class Field<T> implements IStringConverter<T>, IParameterValidator {

  /** @return an object of type T created from the parameter value. */
  @Override
  public abstract T convert(String value);

  /**
   * check the parameter before converting
   *
   * @param name The name of the parameter (e.g. "-host").
   * @param value The value of the parameter that we need to validate. never null or empty
   * @throws ParameterException Thrown if the value of the parameter is invalid.
   */
  protected void check(String name, String value) throws ParameterException {
    // do nothing
  }

  /**
   * Validate the parameter.
   *
   * @param name The name of the parameter (e.g. "-host").
   * @param value The value of the parameter that we need to validate
   * @throws ParameterException Thrown if the value of the parameter is invalid.
   */
  public final void validate(String name, String value) throws ParameterException {
    if (value == null || value.isBlank())
      throw new ParameterException(name + " should not be empty.");
    check(name, value);
  }
}
