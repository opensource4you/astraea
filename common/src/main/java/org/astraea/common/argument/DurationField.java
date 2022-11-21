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
package org.astraea.common.argument;

import java.time.Duration;
import org.astraea.common.Utils;

public class DurationField extends Field<Duration> {

  @Override
  public Duration convert(String input) {
    return Utils.toDuration(input);
  }

  @Override
  protected void check(String name, String value) throws ParameterException {
    if (!Utils.TIME_PATTERN.matcher(value).find())
      throw new ParameterException(
          "field \"" + name + "\"'s value \"" + value + "\" doesn't match time format");
  }
}
