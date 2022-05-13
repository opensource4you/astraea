package org.astraea.web;

import com.beust.jcommander.Parameter;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.astraea.admin.TopicAdmin;
import org.astraea.argument.NonNegativeIntegerField;

public class WebService {

  public static void main(String[] args) throws Exception {
    execute(org.astraea.argument.Argument.parse(new Argument(), args));
  }

  private static void execute(Argument arg) throws IOException {
    var server = HttpServer.create(new InetSocketAddress(arg.port), 0);
    server.createContext("/topics", new TopicHandler(TopicAdmin.of(arg.bootstrapServers())));
    server.createContext("/groups", new GroupHandler(TopicAdmin.of(arg.bootstrapServers())));
    server.start();
  }

  static class Argument extends org.astraea.argument.Argument {
    @Parameter(
        names = {"--port"},
        description = "Integer: the port to bind",
        validateWith = NonNegativeIntegerField.class,
        converter = NonNegativeIntegerField.class)
    int port = 8001;
  }
}
