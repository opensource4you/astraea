package org.astraea.app.web;

import com.beust.jcommander.Parameter;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.astraea.app.admin.Admin;
import org.astraea.app.argument.NonNegativeIntegerField;

public class WebService {

  public static void main(String[] args) throws Exception {
    execute(org.astraea.app.argument.Argument.parse(new Argument(), args));
  }

  private static void execute(Argument arg) throws IOException {
    var server = HttpServer.create(new InetSocketAddress(arg.port), 0);
    server.createContext("/topics", new TopicHandler(Admin.of(arg.configs())));
    server.createContext("/groups", new GroupHandler(Admin.of(arg.configs())));
    server.createContext("/brokers", new BrokerHandler(Admin.of(arg.configs())));
    server.createContext("/producers", new ProducerHandler(Admin.of(arg.configs())));
    server.createContext("/quotas", new QuotaHandler(Admin.of(arg.configs())));
    server.createContext("/pipelines", new PipelineHandler(Admin.of(arg.configs())));
    server.start();
  }

  static class Argument extends org.astraea.app.argument.Argument {
    @Parameter(
        names = {"--port"},
        description = "Integer: the port to bind",
        validateWith = NonNegativeIntegerField.class,
        converter = NonNegativeIntegerField.class)
    int port = 8001;
  }
}
