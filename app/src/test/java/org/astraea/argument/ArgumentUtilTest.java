package org.astraea.argument;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
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

  private static Stream<Arguments> testTimeConverterTestcase() {
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
  @MethodSource("testTimeConverterTestcase")
  public void testTimeConverter(String timeString, Duration expectedDuration) {
    ArgumentUtil.TimeConverter timeConverter = new ArgumentUtil.TimeConverter();

    Assertions.assertEquals(expectedDuration, timeConverter.convert(timeString));
  }
}
