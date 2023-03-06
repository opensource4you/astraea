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
package org.astraea.common.metrics;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.astraea.common.Header;
import org.astraea.common.consumer.Deserializer;

/**
 * Deserialize byte arrays to string and then parse the string to `BeanObject`. It is inverse of
 * BeanObject.toString().getBytes().
 */
public class BeanDeserializer implements Deserializer<BeanObject> {
  @Override
  public BeanObject deserialize(String topic, List<Header> headers, byte[] data) {
    var beanString = new String(data);
    Pattern p =
        Pattern.compile("\\[(?<domain>[^:]*):(?<properties>[^]]*)]\n\\{(?<attributes>[^}]*)}");
    Matcher m = p.matcher(beanString);
    if (!m.matches()) return null;
    var domain = m.group("domain");
    var propertiesPairs = m.group("properties").split("[, ]");
    var attributesPairs = m.group("attributes").split("[, ]");
    var properties =
        Arrays.stream(propertiesPairs)
            .map(kv -> kv.split("="))
            .filter(kv -> kv.length >= 2)
            .collect(Collectors.toUnmodifiableMap(kv -> kv[0], kv -> kv[1]));
    var attributes =
        Arrays.stream(attributesPairs)
            .map(kv -> kv.split("="))
            .filter(kv -> kv.length >= 2)
            .collect(Collectors.toUnmodifiableMap(kv -> kv[0], kv -> (Object) kv[1]));
    return new BeanObject(domain, properties, attributes);
  }
}
