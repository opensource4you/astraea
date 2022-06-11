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

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

/** This class describe the placement state of one kafka log. */
public interface LogPlacement {

  int broker();

  // TODO: remove the Optional thing
  Optional<String> logDirectory();

  static boolean isMatch(List<LogPlacement> sourcePlacements, List<LogPlacement> targetPlacements) {
    if (sourcePlacements.size() != targetPlacements.size()) return false;
    if (sourcePlacements.equals(targetPlacements)) return true;

    final boolean brokerListMatch =
        IntStream.range(0, sourcePlacements.size())
            .allMatch(
                index ->
                    sourcePlacements.get(index).broker() == targetPlacements.get(index).broker());
    if (!brokerListMatch) return false;

    final boolean logDirectoryMatch =
        IntStream.range(0, sourcePlacements.size())
            .allMatch(
                index ->
                    meetLogDirectoryMigrationRequirement(
                        sourcePlacements.get(index).logDirectory(),
                        targetPlacements.get(index).logDirectory()));
    //noinspection RedundantIfStatement
    if (!logDirectoryMatch) return false;

    return true;
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static boolean meetLogDirectoryMigrationRequirement(
      Optional<String> sourceDir, Optional<String> targetDir) {
    // don't care which log directory will eventually be in the destination.
    if (targetDir.isEmpty()) return true;

    // we care which data directory the target will eventually be, but we don't know.
    if (sourceDir.isEmpty()) return false;

    // both candidate is specified, if and only if two paths match, will consider as a requirement
    // meet.
    return sourceDir.get().equals(targetDir.get());
  }

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
