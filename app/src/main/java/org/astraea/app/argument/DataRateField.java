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
package org.astraea.app.argument;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.astraea.common.DataRate;
import org.astraea.common.DataUnit;

public class DataRateField extends Field<DataRate> {

  static final Duration DEFAULT_DURATION = Duration.ofSeconds(1);
  private static final Pattern DATA_RATE_PATTERN =
      Pattern.compile(
          "(?<measurement>\\d+)\\s?(?<dataUnit>[a-zA-Z]+)(/(?<duration>[\\da-z-+.,A-Z]+))?");

  /**
   * Convert string to DataRate.
   *
   * <pre>{@code
   * new DataRateField().convert("500KB/s");  // 500 KB  (500 * 1000 bytes)/ per second
   * new DataRateField().convert("500KiB/H"); // 500 KiB (500 * 1024 bytes)/ per hour
   * new DataRateField().convert("500Kb/minute");  // 500 Kb  (500 * 1000 bits)/ per minute
   * new DataRateField().convert("500Kib/PT-20S"); // 500 Kib (500 * 1024 bits)/ per 20 second
   * }</pre>
   *
   * @param argument number and the unit and duration e.g. "500MiB/s", "9876 KB/PT-20S"
   * @return data size with data unit under duration.
   */
  @Override
  public DataRate convert(String argument) {

    Matcher matcher = DATA_RATE_PATTERN.matcher(argument);
    if (matcher.matches()) {
      var measurement = Long.parseLong(matcher.group("measurement"));
      var dataUnit = DataUnit.valueOf(matcher.group("dataUnit"));
      var duration = getDuration(matcher.group("duration"));
      return new DataRate(dataUnit.of(measurement), duration);
    } else {
      throw new IllegalArgumentException("Unknown DataRate \"" + argument + "\"");
    }
  }

  static Map<String, Duration> DURATION_MAP =
      Map.ofEntries(
          Map.entry("s", Duration.ofSeconds(1)),
          Map.entry("second", Duration.ofSeconds(1)),
          Map.entry("seconds", Duration.ofSeconds(1)),
          Map.entry("m", Duration.ofMinutes(1)),
          Map.entry("minute", Duration.ofMinutes(1)),
          Map.entry("minutes", Duration.ofMinutes(1)),
          Map.entry("h", Duration.ofHours(1)),
          Map.entry("hour", Duration.ofHours(1)),
          Map.entry("hours", Duration.ofHours(1)),
          Map.entry("d", Duration.ofDays(1)),
          Map.entry("day", Duration.ofDays(1)),
          Map.entry("days", Duration.ofDays(1)));

  private Duration getDuration(String duration) {
    if (Objects.isNull(duration)) {
      return DEFAULT_DURATION;
    }
    var lowerCaseDuration = duration.toLowerCase();
    return DURATION_MAP.containsKey(lowerCaseDuration)
        ? DURATION_MAP.get(lowerCaseDuration)
        : Duration.parse(lowerCaseDuration);
  }
}
