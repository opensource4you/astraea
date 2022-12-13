/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.argument;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

public abstract class Field<T> implements IStringConverter<T>, IParameterValidator {

  /**
   * @return an object of type T created from the parameter value.
   */
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
