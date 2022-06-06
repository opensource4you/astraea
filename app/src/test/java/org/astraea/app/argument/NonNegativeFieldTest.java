package org.astraea.app.argument;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NonNegativeFieldTest {

  private static class FakeParameter {
    @Parameter(
        names = {"--field"},
        validateWith = NonNegativeLongField.class)
    public int value = 1;
  }

  @Test
  public void testNotNegative() {
    var param = Argument.parse(new FakeParameter(), new String[] {"--field", "1000"});

    Assertions.assertEquals(1000, param.value);
    Assertions.assertThrows(
        ParameterException.class,
        () -> Argument.parse(new FakeParameter(), new String[] {"--field", "-1"}));
  }
}
