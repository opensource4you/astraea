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
package org.astraea.common;

import java.util.List;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.header.Headers;

public record Header(String key, byte[] value) {
  public static List<Header> of(Headers headers) {
    var iter = headers.iterator();
    // a minor optimization to avoid create extra collection.
    if (!iter.hasNext()) return List.of();
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, 0), false)
        .map(h -> new Header(h.key(), h.value()))
        .toList();
  }

  public static Header of(String key, byte[] value) {
    return new Header(key, value);
  }
}
