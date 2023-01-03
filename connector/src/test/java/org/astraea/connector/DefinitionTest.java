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
package org.astraea.connector;

import java.util.List;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DefinitionTest {

  @Test
  void testRequired() {
    var def = Definition.builder().name("aa").required().build();
    var kafkaConf = Definition.toConfigDef(List.of(def));
    var kafkaDef = kafkaConf.configKeys().entrySet().iterator().next().getValue();
    Assertions.assertEquals(ConfigDef.NO_DEFAULT_VALUE, kafkaDef.defaultValue);
  }

  @Test
  void testOptional() {
    var def = Definition.builder().name("aa").build();
    var kafkaConf = Definition.toConfigDef(List.of(def));
    var kafkaDef = kafkaConf.configKeys().entrySet().iterator().next().getValue();
    Assertions.assertNull(kafkaDef.defaultValue);
  }

  @Test
  void testDefaultType() {
    var def = Definition.builder().name("aa").build();
    var kafkaConf = Definition.toConfigDef(List.of(def));
    var kafkaDef = kafkaConf.configKeys().entrySet().iterator().next().getValue();
    Assertions.assertEquals(ConfigDef.Type.STRING, kafkaDef.type);
  }
}
