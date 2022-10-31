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
package org.astraea.app.automation;

import com.beust.jcommander.Parameter;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.astraea.app.performance.Performance;
import org.astraea.common.argument.NonEmptyStringField;

/**
 * By configuring the parameters in config/automation.properties, control the execution times of
 * performance and its configuration parameters.
 *
 * <ol>
 *   <li>--file: The address of the automation.properties in config folder.
 * </ol>
 */
public class Automation {
  private static final List<String> performanceProperties =
      List.of(
          "--bootstrap.servers",
          "--record.size",
          "--run.until",
          "--compression",
          "--consumers",
          "--fixed.size",
          "--jmx.servers",
          "--partitioner",
          "--partitions",
          "--producers",
          "--prop.file",
          "--replicas",
          "--topic");

  public static void main(String[] args) {
    try {
      var properties = new Properties();
      var arg = org.astraea.common.argument.Argument.parse(new Argument(), args);
      properties.load(new FileInputStream(arg.address));
      var whetherDeleteTopic = properties.getProperty("--whetherDeleteTopic").equals("true");
      var bootstrap = properties.getProperty("--bootstrap.servers");
      var config = new Properties();
      config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);

      var i = 0;
      var times = 0;
      if (properties.getProperty("--time").equals("Default")) times = 5;
      else times = Integer.parseInt(properties.getProperty("--time"));

      while (i < times) {
        var topicName =
            Performance.execute(
                org.astraea.common.argument.Argument.parse(
                    new Performance.Argument(), performanceArgs(properties)));
        i++;
        if (whetherDeleteTopic) {
          try (final AdminClient adminClient = KafkaAdminClient.create(config)) {
            adminClient.deleteTopics(topicName);
          }
        }
        System.out.println("=============== " + i + " time Performance Complete! ===============");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings("unChecked")
  private static String[] performanceArgs(Properties properties) {
    var args = new ArrayList<String>();
    performanceProperties.forEach(
        str -> {
          var property = properties.getProperty(str);
          if (property != null && !property.equals("Default")) {
            args.add(str);
            args.add(property);
          }
        });
    var strings = new String[args.size()];
    return args.toArray(strings);
  }

  private static class Argument {
    @Parameter(
        names = {"--file"},
        description = "String: automation.properties address",
        validateWith = NonEmptyStringField.class)
    String address = "";
  }
}
