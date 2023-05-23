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
package org.astraea.common.serializer;

import java.util.List;
import java.util.Map;
import org.astraea.common.SerializationException;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.producer.Serializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BeanObjectSerializerTest {
  @Test
  public void testSerializationDeserialization() {
    var domain = "domain";
    var properties = Map.of("name", "DifferentType");
    var attributes =
        Map.of(
            "Integer",
            (Object) 1,
            "Long",
            2L,
            "Float",
            (float) 3.4,
            "Double",
            4.4,
            "Boolean",
            true,
            "String",
            "str");
    var time = System.currentTimeMillis();
    var bean = new BeanObject(domain, properties, attributes, time);

    // Valid arguments should not throw
    Assertions.assertDoesNotThrow(
        () -> Serializer.BEAN_OBJECT.serialize("ignore", List.of(), bean));
    var bytes = Serializer.BEAN_OBJECT.serialize("ignore", List.of(), bean);
    Assertions.assertDoesNotThrow(
        () -> Deserializer.BEAN_OBJECT.deserialize("ignore", List.of(), bytes));
    var beanObj = Deserializer.BEAN_OBJECT.deserialize("ignore", List.of(), bytes);

    // Check serialization correctness
    Assertions.assertEquals("domain", beanObj.domainName());
    Assertions.assertEquals(properties, beanObj.properties());
    Assertions.assertEquals(attributes, beanObj.attributes());
    Assertions.assertEquals(time, beanObj.createdTimestamp());
  }

  @Test
  public void testUnsupportedType() {
    var domain = "domain";
    var properties = Map.of("name", "wrongType");
    var attributes = Map.of("unsupportedType", new Object());

    var bean = new BeanObject(domain, properties, attributes);
    var serializedBean = Serializer.BEAN_OBJECT.serialize("ignore", List.of(), bean);
    var deserializedBean =
        Deserializer.BEAN_OBJECT.deserialize("ignore", List.of(), serializedBean);
    // The "map" attribute should be ignored on serialization
    Assertions.assertNotNull(bean.attributes().get("unsupportedType"));
    Assertions.assertNull(deserializedBean.attributes().get("unsupportedType"));
    Assertions.assertNotEquals(bean, deserializedBean);
  }

  @Test
  public void testInvalidBytes() {
    byte[] malformed = new byte[5];
    Assertions.assertThrows(
        SerializationException.class,
        () -> Deserializer.BEAN_OBJECT.deserialize("ignore", List.of(), malformed));
  }
}
