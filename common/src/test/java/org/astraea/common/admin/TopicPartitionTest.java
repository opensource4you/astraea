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
package org.astraea.common.admin;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TopicPartitionTest {

  @Test
  void testConversion() {
    var ourTp = TopicPartition.of("abc", 100);
    var kafkaTp = TopicPartition.to(ourTp);
    Assertions.assertEquals(ourTp.topic(), kafkaTp.topic());
    Assertions.assertEquals(ourTp.partition(), kafkaTp.partition());
  }

  @Test
  void testComparison() {
    var tp0 = TopicPartition.of("abc", 100);
    var tp1 = TopicPartition.of("abc", 101);
    Assertions.assertEquals(-1, tp0.compareTo(tp1));
    Assertions.assertEquals(1, tp1.compareTo(tp0));
    Assertions.assertEquals(0, tp0.compareTo(tp0));
    Assertions.assertEquals(tp0, tp0);
  }

  @Test
  void testString() {
    Assertions.assertEquals(0, TopicPartition.of("a-a", "0").partition());
    Assertions.assertThrows(IllegalArgumentException.class, () -> TopicPartition.of("a", "a"));

    Assertions.assertEquals(0, TopicPartition.of("a-a-0").partition());
    Assertions.assertThrows(IllegalArgumentException.class, () -> TopicPartition.of("a-a"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TopicPartition.of("a-a-"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TopicPartition.of("aaa"));
  }
}
