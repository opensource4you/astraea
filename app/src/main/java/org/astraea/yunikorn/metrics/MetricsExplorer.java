package org.astraea.yunikorn.metrics;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.Timer;

public class MetricsExplorer {
  public static void main(String... argv) throws IOException {
    var args = new Args();
    JCommander.newBuilder().addObject(args).build().parse(argv);
    try {
      Network.exporter();
    } catch (IOException e) {
      e.printStackTrace();
    }
    Timer timer = new Timer();
    Network network = new Network(args.ip, args.controlPlan);
    timer.schedule(network, 0, 1000);
  }

  private static class Args {
    @Parameter(
        names = {"-ip"},
        description = "Address of yunikorn")
    private String ip = "";

    @Parameter(
        names = {"-controlPlan"},
        description = "control plan of kubernetes")
    private String controlPlan = "";
  }
}
