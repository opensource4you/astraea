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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

public interface Header {

  static List<Header> of(Headers headers) {
    return StreamSupport.stream(headers.spliterator(), false)
        .map(h -> of(h.key(), h.value()))
        .collect(Collectors.toList());
  }

  static Headers of(Collection<Header> headers) {
    return new RecordHeaders(
        headers.stream()
            .map(
                h ->
                    new org.apache.kafka.common.header.Header() {
                      @Override
                      public String key() {
                        return h.key();
                      }

                      @Override
                      public byte[] value() {
                        return h.value();
                      }
                    })
            .collect(Collectors.toList()));
  }

  static Header of(String key, byte[] value) {
    return new Header() {
      @Override
      public String key() {
        return key;
      }

      @Override
      public byte[] value() {
        return value;
      }
    };
  }

  String key();

  byte[] value();
}
