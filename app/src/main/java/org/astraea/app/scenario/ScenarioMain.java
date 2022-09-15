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
package org.astraea.app.scenario;

import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.argument.Argument;
import org.astraea.common.cost.Configuration;

public class ScenarioMain extends Argument {

  @Parameter(
      names = {"--scenario.class"},
      required = true)
  String scenarioClass;

  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

  public void execute() {
    var theClass = Utils.packException(() -> Class.forName(scenarioClass));
    if (Scenario.class.isAssignableFrom(theClass)) {
      //noinspection unchecked
      var scenarioClass = (Class<Scenario>) theClass;
      execute(Utils.construct(scenarioClass, Configuration.of(configs())));
    } else {
      throw new RuntimeException("Target class is not a scenario: " + theClass.getName());
    }
  }

  public void execute(Scenario scenario) {
    System.out.println("Accept scenario: " + scenario.getClass().getName());
    try (Admin admin = Admin.of(bootstrapServers())) {
      System.out.println(gson.toJson(scenario.apply(admin)));
    }
  }

  public static void main(String[] args) {
    Argument.parse(new ScenarioMain(), args).execute();
  }
}
