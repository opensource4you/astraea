package org.astraea.argument;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

public interface Field<T> extends IStringConverter<T>, IParameterValidator {

  /** @return an object of type T created from the parameter value. */
  @Override
  T convert(String value);

  /**
   * Validate the parameter.
   *
   * @param name The name of the parameter (e.g. "-host").
   * @param value The value of the parameter that we need to validate
   * @throws ParameterException Thrown if the value of the parameter is invalid.
   */
  void validate(String name, String value) throws ParameterException;
}
