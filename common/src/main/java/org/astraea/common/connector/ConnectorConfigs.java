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
package org.astraea.common.connector;

public final class ConnectorConfigs {
  public static final String NAME_KEY = "name";
  public static final String CONNECTOR_CLASS_KEY = "connector.class";
  public static final String TASK_MAX_KEY = "tasks.max";
  public static final String TOPICS_KEY = "topics";

  public static final String KEY_CONVERTER_KEY = "key.converter";
  public static final String VALUE_CONVERTER_KEY = "value.converter";
  public static final String HEADER_CONVERTER_KEY = "header.converter";

  public static final String BYTE_ARRAY_CONVERTER_CLASS =
      "org.apache.kafka.connect.converters.ByteArrayConverter";

  private ConnectorConfigs() {}
}
