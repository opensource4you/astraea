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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Utils;

public class URLListField extends ListField<URL> {
  @Override
  public List<URL> convert(String value) {
    return Stream.of(value.split(SEPARATOR))
        .map(string -> Utils.packException(() -> new URL(string)))
        .collect(Collectors.toList());
  }

  @Override
  protected void check(String name, String value) {
    super.check(name, value);
    Stream.of(value.split(SEPARATOR))
        .forEach(
            string -> {
              try {
                new URL(string);
              } catch (MalformedURLException e) {
                throw new ParameterException(string + " should be URL");
              }
            });
  }
}
