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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.UnixStyleUsageFormatter;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.astraea.common.Utils;

/** This basic argument defines the common property used by all kafka clients. */
public abstract class Argument {

  static String[] filterEmpty(String[] args) {
    return Arrays.stream(args)
        .map(String::trim)
        .filter(trim -> !trim.isEmpty())
        .toArray(String[]::new);
  }

  /**
   * Side effect: parse args into toolArgument
   *
   * @param toolArgument An argument object that the user want.
   * @param args Command line arguments that are put into main function.
   */
  public static <T> T parse(T toolArgument, String[] args) {
    JCommander jc = JCommander.newBuilder().addObject(toolArgument).build();
    jc.setUsageFormatter(new UnixStyleUsageFormatter(jc));
    try {
      // filter the empty string
      jc.parse(filterEmpty(args));
    } catch (ParameterException pe) {
      var sb = new StringBuilder();
      jc.getUsageFormatter().usage(sb);
      throw new ParameterException(pe.getMessage() + "\n" + sb);
    }
    return toolArgument;
  }

  @Parameter(
      names = {"--bootstrap.servers"},
      description = "String: server to connect to",
      validateWith = NonEmptyStringField.class,
      required = true)
  String bootstrapServers;

  /**
   * @return all configs from both "--configs" and "--prop.file". Other kafka-related configs are
   *     added also.
   */
  public Map<String, String> configs() {
    var all = new HashMap<>(configs);
    if (propFile != null) {
      var props = new Properties();
      Utils.packException(
          () -> {
            try (var input = new FileInputStream(propFile)) {
              props.load(input);
            }
          });
      props.forEach((k, v) -> all.put(k.toString(), v.toString()));
    }
    all.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    return Collections.unmodifiableMap(all);
  }

  public String bootstrapServers() {
    return bootstrapServers;
  }

  @Parameter(
      names = {"--prop.file"},
      description = "the file path containing the properties to be passed to kafka admin",
      validateWith = NonEmptyStringField.class)
  String propFile;

  @Parameter(
      names = {"--configs"},
      description = "Map: set configs by command-line. For example: --configs a=b,c=d",
      converter = StringMapField.class,
      validateWith = StringMapField.class)
  Map<String, String> configs = Map.of();
}
