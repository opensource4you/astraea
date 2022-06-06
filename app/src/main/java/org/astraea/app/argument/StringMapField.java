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

import com.beust.jcommander.ParameterException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class StringMapField extends Field<Map<String, String>> {

  @Override
  public Map<String, String> convert(String value) {
    return Arrays.stream(value.split(","))
        .map(
            item -> {
              var keyValue = item.split("=");
              if (keyValue.length != 2) throw new ParameterException("incorrect format: " + item);
              return Map.entry(keyValue[0], keyValue[1]);
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  protected void check(String name, String value) throws ParameterException {
    if (!value.contains("=") || convert(value).isEmpty())
      throw new ParameterException("incorrect format: " + value);
  }
}
