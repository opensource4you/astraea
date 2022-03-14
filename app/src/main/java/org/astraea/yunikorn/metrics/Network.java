package org.astraea.yunikorn.metrics;

import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.astraea.yunikorn.metrics.Infos.Info;
import org.astraea.yunikorn.metrics.Infos.Node;
import org.json.JSONArray;
import org.json.JSONException;

public class Network {
  public static final String NODE_KEY = "nodesInfo";

  public Info getInfo(String ip, Info info) {
    HttpClient client =
        HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(20))
            .build();
    HttpRequest request =
        HttpRequest.newBuilder().uri(URI.create("http://" + ip + "/ws/v1/nodes")).GET().build();
    List<Node> nodes = new ArrayList<>();
    try {
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      var body = response.body();
      nodes = getNodes(body);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    info.update(nodes);
    return info;
  }

  public static void exporter() throws IOException {
    HTTPServer server = new HTTPServer.Builder().withPort(9999).build();
  }

  public List<Node> getNodes(String body) {
    List<Node> nodes = new ArrayList<>();
    try {
      var json = new JSONArray(body);
      for (int i = 0; i < json.getJSONObject(0).getJSONArray(NODE_KEY).length(); i++) {
        var node = new Node(json.getJSONObject(0).getJSONArray(NODE_KEY).getJSONObject(i));
        nodes.add(node);
      }

    } catch (JSONException e) {
      e.printStackTrace();
    }
    return nodes;
  }
}
