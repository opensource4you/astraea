package org.astraea.argument;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PositiveFieldTest {

  private static class FakeParameter {
    @Parameter(
        names = {"--field"},
        validateWith = PositiveLongField.class)
    public long value = 1;
  }

  @Test
  public void testNotNegative() {
    var param =
        org.astraea.argument.Argument.parse(new FakeParameter(), new String[] {"--field", "1000"});

    Assertions.assertEquals(1000, param.value);
    Assertions.assertThrows(
        ParameterException.class,
        () ->
            org.astraea.argument.Argument.parse(
                new FakeParameter(), new String[] {"--field", "0"}));
    Assertions.assertThrows(
        ParameterException.class,
        () ->
            org.astraea.argument.Argument.parse(
                new FakeParameter(), new String[] {"--field", "-1"}));
  }
}
