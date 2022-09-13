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

import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.cost.Configuration;

/**
 * The subclass of this class should contain the logic to fulfill a scenario.
 *
 * @param <T> the return result after applying this scenario to the specific Kafka cluster. This
 *     result might be print on the program output. Or being serialized to json and transmit by web
 *     API.
 */
public abstract class Scenario<T> {

  /**
   * This field is here for serialization purpose. It should be the class name of the concrete
   * scenario implementation. Gson can take advantage of this field to classify which scenario class
   * it should deserialize to.
   */
  private final String scenarioName;

  /** @param classPath the classpath to the concrete implementation of the scenario */
  public Scenario(String classPath) {
    this.scenarioName = classPath;
  }

  public static Scenario<?> of(Class<Scenario<?>> theClass, Configuration configuration) {
    return Utils.construct(theClass, configuration);
  }

  /** Apply this scenario to the Kafka cluster */
  public abstract T apply(Admin admin);
}
