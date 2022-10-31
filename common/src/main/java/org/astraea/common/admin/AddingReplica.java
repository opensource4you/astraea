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

public interface AddingReplica {

  static AddingReplica of(
      String topic, int partition, int broker, String path, long size, long leaderSize) {
    return new AddingReplica() {

      @Override
      public String topic() {
        return topic;
      }

      @Override
      public int partition() {
        return partition;
      }

      @Override
      public int broker() {
        return broker;
      }

      @Override
      public String path() {
        return path;
      }

      @Override
      public long size() {
        return size;
      }

      @Override
      public long leaderSize() {
        return leaderSize;
      }
    };
  }

  String topic();

  int partition();

  int broker();

  String path();

  /**
   * @return current data size of this adding replica
   */
  long size();

  /**
   * @return size (of topic partition leader) to sync
   */
  long leaderSize();
}
