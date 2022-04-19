package org.astraea.yunikorn;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.astraea.yunikorn.client.Client;

import java.util.Timer;

public class Yunikorn {
  public static void main(String... argv) {
    var args = new Args();
    JCommander.newBuilder().addObject(args).build().parse(argv);
    var client = new Client(args.ip);
    Timer timer = new Timer();
    timer.schedule(client, 0, 1000);
  }

  private static class Args {
    @Parameter(
            names = {"-ip"},
            description = "Address of yunikorn")
    private String ip = "";
  }
}