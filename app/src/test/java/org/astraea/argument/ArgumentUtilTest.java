package org.astraea.argument;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
}
