package org.astraea.argument;

import com.beust.jcommander.ParameterException;
import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ArgumentUtilTest {
  @Test
  public void testParse() {
    Assertions.assertTrue(
        ArgumentUtil.checkArgument(
            Arrays.asList(
                "End2EndLatency",
                "--bootstrap.servers",
                "localhost:9092",
                "--topic",
                "testing",
                "--producers",
                "3",
                "--consumers",
                "4",
                "--duration",
                "5",
                "--valueSize",
                "6",
                "--flushDuration",
                "7")));
  }

  @Test
  public void testArgumentLost() {
    Assertions.assertEquals(
        "--bootstrap.servers should not be empty.",
        Assertions.assertThrows(
                ParameterException.class,
                () ->
                    ArgumentUtil.checkArgument(
                        Arrays.asList(
                            "OffsetExplorer", "--bootstrap.servers", "", "--topic", "testing")))
            .getMessage());
    Assertions.assertEquals(
        "--topic should not be empty.",
        Assertions.assertThrows(
                ParameterException.class,
                () ->
                    ArgumentUtil.checkArgument(
                        Arrays.asList(
                            "OffsetExplorer",
                            "--bootstrap.servers",
                            "localhost:9092",
                            "--topic",
                            "")))
            .getMessage());
  }

  @Test
  public void testInvalidArgument() {
    Assertions.assertEquals(
        "--producers should be positive.",
        Assertions.assertThrows(
                ParameterException.class,
                () ->
                    ArgumentUtil.checkArgument(
                        Arrays.asList(
                            "End2EndLatency",
                            "--bootstrap.servers",
                            "localhost:9092",
                            "--topic",
                            "test",
                            "--producers",
                            "0")))
            .getMessage());
    Assertions.assertEquals(
        "--consumers should not be negative.",
        Assertions.assertThrows(
                ParameterException.class,
                () ->
                    ArgumentUtil.checkArgument(
                        Arrays.asList(
                            "End2EndLatency",
                            "--bootstrap.servers",
                            "localhost:9092",
                            "--topic",
                            "test",
                            "--consumers",
                            "-1")))
            .getMessage());
    Assertions.assertEquals(
        "--duration should be positive.",
        Assertions.assertThrows(
                ParameterException.class,
                () ->
                    ArgumentUtil.checkArgument(
                        Arrays.asList(
                            "End2EndLatency",
                            "--bootstrap.servers",
                            "localhost:9092",
                            "--topic",
                            "test",
                            "--duration",
                            "0")))
            .getMessage());
    Assertions.assertEquals(
        "--valueSize should be positive.",
        Assertions.assertThrows(
                ParameterException.class,
                () ->
                    ArgumentUtil.checkArgument(
                        Arrays.asList(
                            "End2EndLatency",
                            "--bootstrap.servers",
                            "localhost:9092",
                            "--topic",
                            "test",
                            "--valueSize",
                            "0")))
            .getMessage());
    Assertions.assertEquals(
        "--flushDuration should be positive.",
        Assertions.assertThrows(
                ParameterException.class,
                () ->
                    ArgumentUtil.checkArgument(
                        Arrays.asList(
                            "End2EndLatency",
                            "--bootstrap.servers",
                            "localhost:9092",
                            "--topic",
                            "test",
                            "--flushDuration",
                            "0")))
            .getMessage());
  }
}
