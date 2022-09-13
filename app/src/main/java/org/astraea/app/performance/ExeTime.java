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
package org.astraea.app.performance;

import com.beust.jcommander.ParameterException;
import java.time.Duration;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import org.astraea.common.argument.DurationField;

/**
 * Two kind of running modes. One runs for a duration of time. The other runs for a number of
 * records.
 */
interface ExeTime {

  double percentage(long records, long elapsedTime);

  static ExeTime of(String exeTime) {
    if (exeTime.endsWith("records")) {
      final long records = Long.parseLong(exeTime.replace("records", ""));
      return ExeTime.of((completeRecords, ignore) -> 100D * completeRecords / records, exeTime);
    }
    final Duration duration = new DurationField().convert(exeTime);
    return ExeTime.of((ignore, elapsedTime) -> 100D * elapsedTime / duration.toMillis(), exeTime);
  }

  static ExeTime of(BiFunction<Long, Long, Double> function, String toString) {
    return new ExeTime() {
      @Override
      public double percentage(long records, long duration) {
        return function.apply(records, duration);
      }

      @Override
      public String toString() {
        return toString;
      }
    };
  }

  class Field extends org.astraea.common.argument.Field<ExeTime> {
    static final Pattern PATTERN = Pattern.compile("^([0-9]+)(days|day|h|m|s|ms|us|ns|records)$");

    @Override
    protected void check(String name, String value) throws ParameterException {
      if (!PATTERN.matcher(value).matches()) {
        throw new ParameterException(
            "Invalid ExeTime format. valid format example: \"1m\" or \"89242records\"");
      }
    }

    @Override
    public ExeTime convert(String value) {
      return ExeTime.of(value);
    }
  }
}
