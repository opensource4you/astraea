package org.astraea.argument;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.time.Duration;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

public class ArgumentUtilTest {
  public static class FakeParameter {
    @Parameter(
        names = {"--require"},
        validateWith = ArgumentUtil.NotEmptyString.class,
        required = true)
    public String require;

    @Parameter(
        names = {"--longPositive"},
        validateWith = ArgumentUtil.PositiveLong.class)
    public long longPositive;

    @Parameter(
        names = {"--longNotNegative"},
        validateWith = ArgumentUtil.NonNegativeLong.class)
    public int longNotNegative = 1;

    @Parameter(
        names = {"--durationConvert"},
        converter = ArgumentUtil.DurationConverter.class)
    public Duration durationConvert;

    @Parameter(
        names = {"--setConverter"},
        converter = ArgumentUtil.StringSetConverter.class,
        variableArity = true)
    public Set<String> setConverter;
  }

  @Test
  public void testParse() {
    var param =
        ArgumentUtil.parseArgument(new FakeParameter(), new String[] {"--require", "require"});
    Assertions.assertEquals("require", param.require);
  }

  @Test
  public void testRequired() {
    Assertions.assertThrows(
        ParameterException.class,
        () -> ArgumentUtil.parseArgument(new FakeParameter(), new String[] {}));
  }

  @Test
  public void testLongPositive() {
    var param =
        ArgumentUtil.parseArgument(
            new FakeParameter(), new String[] {"--require", "require", "--longPositive", "1000"});

    Assertions.assertEquals(1000, param.longPositive);
    Assertions.assertThrows(
        ParameterException.class,
        () ->
            ArgumentUtil.parseArgument(
                new FakeParameter(), new String[] {"--require", "require", "--longPositive", "0"}));
  }

  @Test
  public void testNotNegative() {
    FakeParameter param =
        ArgumentUtil.parseArgument(
            new FakeParameter(),
            new String[] {"--require", "require", "--longNotNegative", "1000"});

    Assertions.assertEquals(1000, param.longNotNegative);
    Assertions.assertThrows(
        ParameterException.class,
        () ->
            ArgumentUtil.parseArgument(
                new FakeParameter(),
                new String[] {"--require", "require", "--longNotNegative", "-1"}));
  }

  @Test
  public void testDurationConvert() {
    FakeParameter param =
        ArgumentUtil.parseArgument(
            new FakeParameter(),
            new String[] {"--require", "require", "--durationConvert", "1000"});

    Assertions.assertEquals(Duration.ofSeconds(1000), param.durationConvert);
  }

  @Test
  public void testSetConverter() {
    FakeParameter param =
        ArgumentUtil.parseArgument(
            new FakeParameter(),
            new String[] {"--require", "require", "--setConverter", "1", "1", "2"});

    Assertions.assertEquals(Set.of("1", "2"), param.setConverter);
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

  @ParameterizedTest(name = "[{index}] time string \"{0}\" will match duration \"{1}\"")
  @MethodSource("testDurationConvertorTestcases")
  public void testDurationConvertorConvert(String timeString, Duration expectedDuration) {
    ArgumentUtil.DurationConverter durationConverter = new ArgumentUtil.DurationConverter();

    Assertions.assertEquals(expectedDuration, durationConverter.convert(timeString));
  }

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
                ArgumentUtil.DurationConverter durationConverter =
                    new ArgumentUtil.DurationConverter();
                durationConverter.validate("key", timeString);
                return true;
              } catch (ParameterException ignored) {
                return false;
              }
            };

    Assertions.assertEquals(isLegal, execution.get());
  }

  @Test
  void testValidPath() {
    var validPath = new ArgumentUtil.ValidPath();
    Assertions.assertDoesNotThrow(() -> validPath.validate("ignore", "dir"));
    Assertions.assertDoesNotThrow(() -> validPath.validate("ignore", "./dir"));
    Assertions.assertDoesNotThrow(() -> validPath.validate("ignore", "/path/to/desired/dir"));
    Assertions.assertThrows(ParameterException.class, () -> validPath.validate("ignore", "///"));
  }
}
