package org.astraea.yunikorn.metrics;

import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import org.astraea.yunikorn.config.NodeSortingPolicy;
import org.astraea.yunikorn.metrics.Infos.Application;
import org.astraea.yunikorn.metrics.Infos.Info;
import org.astraea.yunikorn.metrics.Infos.Node;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Network extends TimerTask {
  public static final String NODE_KEY = "nodesInfo";
  private String ip;
  private String controlPlan;
  private Info info = new Info();
  private HttpClient client;
  private HttpRequest nodesRequest;
  private HttpRequest appsRequest;

  public Network(String ip, String controlPlan) {

    this.client =
        HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .connectTimeout(Duration.ofSeconds(20))
            .build();

    this.nodesRequest =
        HttpRequest.newBuilder().uri(URI.create("http://" + ip + "/ws/v1/nodes")).GET().build();
    this.appsRequest =
        HttpRequest.newBuilder().uri(URI.create("http://" + ip + "/ws/v1/apps")).GET().build();
    this.ip = ip;
    this.controlPlan = controlPlan;
    info.register();
  }

  private static HTTPServer server;

  @Override
  public void run() {

    try {
      HttpResponse<String> response =
          client.send(nodesRequest, HttpResponse.BodyHandlers.ofString());
      var body = response.body();
      List<Node> nodes = getNodes(body, controlPlan);

      response = client.send(appsRequest, HttpResponse.BodyHandlers.ofString());
      body = response.body();
      List<Application> apps = getApps(body);

      info.update(nodes, apps);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void exporter() throws IOException {
    HTTPServer server = new HTTPServer.Builder().withPort(9990).build();
    Network.server = server;
  }

  public List<Node> getNodes(String body, String controlPlan) {
    if (body == null) {
      System.out.println("body of applications doesn't exist");
      return null;
    }

    List<Node> nodes = new ArrayList<>();
    try {
      var json = new JSONArray(body);
      for (int i = 0; i < json.getJSONObject(0).getJSONArray(NODE_KEY).length(); i++) {
        if (json.getJSONObject(0)
                .getJSONArray(NODE_KEY)
                .getJSONObject(i)
                .get(Info.NODEID)
                .toString()
                .compareTo(controlPlan)
            == 0) continue;

        var nodeID =
            json.getJSONObject(0)
                .getJSONArray(NODE_KEY)
                .getJSONObject(i)
                .get(Info.NODEID)
                .toString();
        var hostName =
            json.getJSONObject(0)
                .getJSONArray(NODE_KEY)
                .getJSONObject(i)
                .get(Node.HOSTNAME_KEY)
                .toString();
        var rackName =
            json.getJSONObject(0)
                .getJSONArray(NODE_KEY)
                .getJSONObject(i)
                .get(Node.RACKNAME_KEY)
                .toString();

        Map<String, BigInteger> capacity =
            unmarshall(
                json.getJSONObject(0)
                    .getJSONArray(NODE_KEY)
                    .getJSONObject(i)
                    .getJSONObject(Node.CAPACITY_KEY));
        Map<String, BigInteger> available =
            unmarshall(
                json.getJSONObject(0)
                    .getJSONArray(NODE_KEY)
                    .getJSONObject(i)
                    .getJSONObject(Node.AVAILABLE_KEY));
        var node = new Node(nodeID, hostName, rackName, capacity, available);
        nodes.add(node);
      }

    } catch (JSONException e) {
      e.printStackTrace();
    }
    return nodes;
  }

  public List<Application> getApps(String body) {
    if (body == null) {
      System.out.println("body of applications doesn't exist");
      return null;
    }

    List<Application> apps = new ArrayList<>();
    try {
      var json = new JSONArray(body);
      for (int i = 0; i < json.length(); i++) {
        var applicationState = json.getJSONObject(i).get("applicationState").toString();
        if (applicationState.compareTo("New") == 0 || applicationState.compareTo("Accepted") == 0) {
          continue;
        }
        var applicationID = json.getJSONObject(i).get("applicationID").toString();
        var partition = json.getJSONObject(i).get("partition").toString();
        var queueName = json.getJSONObject(i).get("queueName").toString();

        var tmp = json.getJSONObject(i).get("submissionTime").toString();
        if (tmp.compareTo("null") == 0) {
          tmp = "0";
        }
        var submissionTime = Long.valueOf(tmp);
        tmp = json.getJSONObject(i).get("finishedTime").toString();

        if (tmp.compareTo("null") == 0) {
          tmp = "0";
        }
        var finishedTime = Long.valueOf(tmp);

        var app =
            new Application(
                applicationID,
                partition,
                queueName,
                submissionTime.longValue(),
                finishedTime.longValue(),
                applicationState);
        apps.add(app);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return apps;
  }

  private Map<String, BigInteger> unmarshall(JSONObject json) {
    Map<String, BigInteger> source = new HashMap<>();
    try {
      source.put(
          NodeSortingPolicy.MEMORY_KEY,
          BigInteger.valueOf(json.getLong(NodeSortingPolicy.MEMORY_KEY)));
      source.put(
          NodeSortingPolicy.CORE_KEY, BigInteger.valueOf(json.getLong(NodeSortingPolicy.CORE_KEY)));
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return source;
  }
}
