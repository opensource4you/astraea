package org.astraea.yunikorn;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.util.Timer;

import org.astraea.argument.NonEmptyStringField;
import org.astraea.yunikorn.client.YunikornClient;

public class Yunikorn {
  public static void main(String... argv) {
    var args = new Argument();
    JCommander.newBuilder().addObject(args).build().parse(argv);
    var client = new YunikornClient(args.ip);
    Timer timer = new Timer();
    timer.schedule(client, 0, 1000);
  }

  private static class Argument {
    @Parameter(
        names = {"-ip"},
        description = "Address of yunikorn",
        validateWith = NonEmptyStringField.class,
        required = true)
    public String ip = "0.0.0.0:9080";
  }
}
