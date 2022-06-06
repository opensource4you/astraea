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
package org.astraea.app.balancer.log;

import java.util.Optional;

/** This class describe the placement state of one kafka log. */
public interface LogPlacement {

  int broker();

  Optional<String> logDirectory();

  static LogPlacement of(int broker) {
    return of(broker, null);
  }

  static LogPlacement of(int broker, String logDirectory) {
    return new LogPlacement() {
      @Override
      public int broker() {
        return broker;
      }

      @Override
      public Optional<String> logDirectory() {
        return Optional.ofNullable(logDirectory);
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof LogPlacement) {
          final var that = (LogPlacement) obj;
          return this.broker() == that.broker() && this.logDirectory().equals(that.logDirectory());
        }
        return false;
      }

      @Override
      public String toString() {
        return "LogPlacement{broker=" + broker() + " logDir=" + logDirectory() + "}";
      }
    };
  }
}
