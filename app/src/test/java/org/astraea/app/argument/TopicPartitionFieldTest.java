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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TopicPartitionFieldTest {

  @Test
  void convert() {
    var field = new TopicPartitionField();

    Assertions.assertEquals("Apple", field.convert("Apple-1").topic());
    Assertions.assertEquals(1, field.convert("Apple-1").partition());
    Assertions.assertEquals("document-store", field.convert("document-store-5").topic());
    Assertions.assertEquals(5, field.convert("document-store-5").partition());
  }
}
