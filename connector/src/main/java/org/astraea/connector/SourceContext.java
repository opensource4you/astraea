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

import java.util.Map;
import java.util.stream.Collectors;

public interface SourceContext {

  SourceContext EMPTY =
      new SourceContext() {
        @Override
        public void raiseError(Exception e) {}

        @Override
        public Map<String, String> metadata(Map<String, String> index) {
          return Map.of();
        }
      };

  static SourceContext of(org.apache.kafka.connect.source.SourceConnectorContext context) {
    return new SourceContext() {
      @Override
      public void raiseError(Exception e) {
        context.raiseError(e);
      }

      @Override
      public Map<String, String> metadata(Map<String, String> index) {
        var v = context.offsetStorageReader().offset(index);
        if (v == null) return Map.of();
        return v.entrySet().stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> e.getValue().toString()));
      }
    };
  }

  /** {@link org.apache.kafka.connect.source.SourceConnectorContext#raiseError(Exception)} */
  void raiseError(Exception e);

  Map<String, String> metadata(Map<String, String> index);
}
