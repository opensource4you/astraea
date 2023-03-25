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

import java.util.Objects;

public class BrokerPath {

  private final int broker;
  private final String path;

  public static BrokerPath of(int broker, String path) {
    return new BrokerPath(broker, path);
  }

  public BrokerPath(int broker, String path) {
    this.broker = broker;
    this.path = path;
  }

  public int broker() {
    return broker;
  }

  public String path() {
    return path;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BrokerPath that = (BrokerPath) o;
    return broker == that.broker && Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(broker, path);
  }

  @Override
  public String toString() {
    return "BrokerPath{" + "broker=" + broker + ", path='" + path + '\'' + '}';
  }
}
