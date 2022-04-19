package org.astraea.yunikorn.client;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.astraea.yunikorn.config.NodeSortingPolicy;
import org.astraea.yunikorn.config.SchedulerConfig;
import org.astraea.yunikorn.core.Evaluation;
import org.json.JSONArray;
import org.json.JSONException;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class Client extends TimerTask {
  private SchedulerConfig schedulerConfig;
  private Evaluation evaluation = new Evaluation();
  private HttpClient client;
  private String ip;
  private ExecutorService executor;
  private int i;

  public Client(String ip) {
    executor = Executors.newSingleThreadExecutor();
    this.client =
        HttpClient.newBuilder()
            .followRedirects(Redirect.ALWAYS)
            .connectTimeout(Duration.ofSeconds(20))
            .executor(executor)
            .build();
    this.ip = ip;
  }

  public void getPartitionConfig() {
    HttpRequest request =
        HttpRequest.newBuilder().uri(URI.create("http://" + ip + "/ws/v1/config")).GET().build();

    try {
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == 200) {
        var body = response.body();
        var yaml = new Yaml(new Constructor(SchedulerConfig.class));
        this.schedulerConfig = (SchedulerConfig) yaml.load(body);
      } else {
        System.out.println(response.statusCode());
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return;
  }

  public Boolean listen() {
    HttpRequest request =
        HttpRequest.newBuilder().uri(URI.create("http://" + ip + "/ws/v1/apps")).GET().build();
    try {
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == 200) {
        var body = response.body();
        if (getStartingApplication(body) == 0) {
          return false;
        }
      } else {
        System.out.println(response.statusCode());
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return true;
  }

  private int getStartingApplication(String body) {
    var totalRunning = 0;
    try {
      var json = new JSONArray(body);
      var totalApplication = json.length();
      var totalMemory = BigInteger.ZERO;
      var totalCore = BigInteger.ZERO;
      for (int i = 0; i < totalApplication; i++) {
        var application = json.getJSONObject(i);
        var state = application.get("applicationState").toString();
        if (state.compareTo("Running") == 0 || state.compareTo("Starting") == 0) {
          var memory =
              new BigInteger(application.getJSONObject("usedResource").get("memory").toString());
          var core =
              new BigInteger(application.getJSONObject("usedResource").get("vcore").toString());
          totalMemory = totalMemory.add(memory);
          totalCore = totalCore.add(core);
          totalRunning++;
        }
      }
      this.evaluation.setResources(totalMemory.toString(), totalMemory.toString());

    } catch (JSONException e) {
      e.printStackTrace();
    }
    return totalRunning;
  }

  public void adjust() {
    HttpRequest getRequest =
        HttpRequest.newBuilder()
            .uri(URI.create("http://" + ip + "/ws/v1/clusters/utilization"))
            .GET()
            .build();
    try {
      HttpResponse<String> response = client.send(getRequest, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == 200) {
        var body = response.body();
        getUtilization(body);
        schedulerConfig
            .getPartitions()
            .get(0)
            .getNodesortpolicy()
            .putResourceweights(NodeSortingPolicy.CORE_KEY, evaluation.calculate());
        schedulerConfig
            .getPartitions()
            .get(0)
            .getNodesortpolicy()
            .putResourceweights(NodeSortingPolicy.MEMORY_KEY, 1.0);
      } else {
        System.out.println(response.statusCode());
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    var yaml = new Yaml(new Constructor(SchedulerConfig.class));
    HttpRequest putRequest =
        HttpRequest.newBuilder()
            .uri(URI.create("http://" + ip + "/ws/v1/config"))
            .PUT(HttpRequest.BodyPublishers.ofString(yaml.dump(this.schedulerConfig)))
            .build();
    try {
      HttpResponse<String> response = client.send(putRequest, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == 200) {
        System.out.printf("number %d %s\n", i, response.body());
        i++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void getUtilization(String body) {
    JSONArray json = null;
    try {
      json = new JSONArray(body);
      var totalMemory =
          Long.valueOf(
              json.getJSONObject(0)
                  .getJSONArray("utilization")
                  .getJSONObject(0)
                  .get("total")
                  .toString());
      var usedMemory =
          Long.valueOf(
              json.getJSONObject(0)
                  .getJSONArray("utilization")
                  .getJSONObject(0)
                  .get("used")
                  .toString());
      var totalVCore =
          Long.valueOf(
              json.getJSONObject(0)
                  .getJSONArray("utilization")
                  .getJSONObject(1)
                  .get("total")
                  .toString());
      var usedVCore =
          Long.valueOf(
              json.getJSONObject(0)
                  .getJSONArray("utilization")
                  .getJSONObject(1)
                  .get("used")
                  .toString());
      this.evaluation.setUnused(
          Long.valueOf(totalMemory - usedMemory).toString(),
          Long.valueOf(totalVCore - usedVCore).toString());
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    getPartitionConfig();
    if (listen()) {
      adjust();
    }
  }
}
