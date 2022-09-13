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
package org.astraea.common.producer;

import java.util.Arrays;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.astraea.common.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AcksTest {

  @Test
  void testKafkaConfig() {
    var config =
        ((ConfigDef) Utils.staticMember(ProducerConfig.class, "CONFIG"))
            .configKeys()
            .get(ProducerConfig.ACKS_CONFIG);
    Arrays.stream(Acks.values())
        .forEach(ack -> config.validator.ensureValid("acks", ack.valueOfKafka()));
  }

  @Test
  void testAlias() {
    Arrays.stream(Acks.values())
        .forEach(acks -> Assertions.assertEquals(acks, Acks.ofAlias(acks.alias())));
  }

  @Test
  void testField() {
    var field = new Acks.Field();
    Arrays.stream(Acks.values())
        .forEach(acks -> Assertions.assertEquals(acks, field.convert(acks.alias())));
  }
}
