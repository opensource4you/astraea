package org.astraea.argument;

import com.beust.jcommander.Parameter;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SetFieldTest {
  private static class FakeParameter {
    @Parameter(
        names = {"--field"},
        converter = StringSetField.class,
        variableArity = true)
    public Set<String> value;
  }

  @Test
  public void testSetConverter() {
    var param =
        org.astraea.argument.Argument.parse(new FakeParameter(), new String[] {"--field", "1,2,3"});

    Assertions.assertEquals(Set.of("1", "2", "3"), param.value);
  }
}
