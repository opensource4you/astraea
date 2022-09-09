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
package org.astraea.common.argument;

import com.beust.jcommander.ParameterException;
import java.time.Duration;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

public class DurationFieldTest {
  @ParameterizedTest()
  @CsvSource(
      delimiterString = ",",
      value = {
        // input string, is_legal, test-purpose
        "             0, true    , valid unit",
        "             1, true    , valid unit",
        "          5566, true    , valid unit",
        "        1234ns, true    , valid unit",
        "        4321us, true    , valid unit",
        "        1234ms, true    , valid unit",
        "        12000s, true    , valid unit",
        "           60m, true    , valid unit",
        "           60h, true    , valid unit",
        "        365day, true    , valid unit",
        "       365days, true    , valid unit",
        "   0010100days, true    , valid unit",
        "         -1234, false   , currently no negative number allowed",
        "       -1234ms, false   , currently no negative number allowed",
        "      -365days, false   , currently no negative number allowed",
        "          0.5s, false   , currently no floating value allowed",
        "         hello, false   , illegal time/unit",
        "            ms, false   , illegal time/unit",
        "           day, false   , illegal time/unit",
        "             h, false   , illegal time/unit",
      })
  public void testDurationConvertorValidate(String timeString, boolean isLegal) {
    var execution =
        (Supplier<Boolean>)
            () -> {
              try {
                var durationConverter = new DurationField();
                durationConverter.validate("key", timeString);
                return true;
              } catch (ParameterException ignored) {
                return false;
              }
            };

    Assertions.assertEquals(isLegal, execution.get());
  }

  @ParameterizedTest(name = "[{index}] time string \"{0}\" will match duration \"{1}\"")
  @MethodSource("testDurationConvertorTestcases")
  public void testDurationConvertorConvert(String timeString, Duration expectedDuration) {
    var durationConverter = new DurationField();

    Assertions.assertEquals(expectedDuration, durationConverter.convert(timeString));
  }

  private static Stream<Arguments> testDurationConvertorTestcases() {
    return Stream.of(
        Arguments.of("1", Duration.ofSeconds(1)),
        Arguments.of("0", Duration.ZERO),
        Arguments.of("60s", Duration.ofSeconds(60)),
        Arguments.of("30m", Duration.ofMinutes(30)),
        Arguments.of("24h", Duration.ofHours(24)),
        Arguments.of("7day", Duration.ofDays(7)),
        Arguments.of("7days", Duration.ofDays(7)),
        Arguments.of("100ms", Duration.ofMillis(100)),
        Arguments.of("500us", Duration.ofNanos(500 * 1000)),
        Arguments.of("1ns", Duration.ofNanos(1)));
  }
}
