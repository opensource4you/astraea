package org.astraea.yunikorn.client;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.astraea.yunikorn.config.NodeSortingPolicy;
import org.json.JSONArray;
import org.json.JSONException;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import java.time.Duration;
import org.astraea.yunikorn.core.Evaluation;
import org.astraea.yunikorn.config.SchedulerConfig;

public class Client {
    private SchedulerConfig schedulerConfig ;
    private Evaluation evaluation = new Evaluation();
    public void getPartitionConfig(String ip){
        HttpClient client = HttpClient.newBuilder()
                .followRedirects(Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(20))
                .build();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://"+ip+"/ws/v1/config"))
                .GET()
                .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode()==200){
                var body = response.body();
                var yaml = new Yaml(new Constructor(SchedulerConfig.class));
                this.schedulerConfig  = (SchedulerConfig) yaml.load(body);
            }
            else {
                System.out.println(response.statusCode());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return;
    }

    public Boolean listen(String ip){
        HttpClient client = HttpClient.newBuilder()
                .followRedirects(Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(20))
                .build();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://"+ip+"/ws/v1/apps"))
                .GET()
                .build();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode()==200) {
                var body = response.body();
                if(getApplication(body)==0){
                    return false;
                }
            }
            else {
                System.out.println(response.statusCode());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }
    private int getApplication(String body){
        var totalRunning = 0;
        try {
            var json = new JSONArray(body);
            var totalApplication = json.length();
            long[] totalResources = {0, 0};
            for (int i= 0;i<totalApplication;i++){
                var application = json.getJSONObject(i);
                String state= application
                        .get("applicationState")
                        .toString();
                var resources = application
                        .get("usedResource")
                        .toString();
                if (resources.length()<=3)
                    continue;
                if(state.compareTo("Starting")==0){
                    resources = resources.substring(1, resources.length()-1);
                    String[] pairs = resources.split(" ");
                    totalRunning++;
                    for(int j = 0;j<pairs.length;j++){
                        var pair = pairs[j];
                        String[] keyValue = pair.split(":");
                        totalResources[j] = totalResources[j]+Long.valueOf(keyValue[1]);
                    }
                }
            }
            this.evaluation.setResources(Long.valueOf(totalResources[0]).toString(), Long.valueOf(totalResources[1]).toString());

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return totalRunning;
    }
    public void adjust(String ip){
        HttpClient client = HttpClient.newBuilder()
                .followRedirects(Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(20))
                .build();
        HttpRequest getRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://"+ip+"/ws/v1/clusters/utilization"))
                .GET()
                .build();
        try {
            HttpResponse<String> response = client.send(getRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode()==200){
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
                        .putResourceweights(NodeSortingPolicy.MEMORY_KEY,1.0);
            }
            else {
                System.out.println(response.statusCode());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        client = HttpClient.newBuilder()
                .followRedirects(Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(20))
                .build();
        var yaml = new Yaml(new Constructor(SchedulerConfig.class));
        HttpRequest putRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://"+ip+"/ws/v1/config"))
                .PUT(HttpRequest.BodyPublishers.ofString(yaml.dump(this.schedulerConfig)))
                .build();
        try {
            HttpResponse<String> response = client.send(putRequest, HttpResponse.BodyHandlers.ofString());
            System.out.println(response.body());

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
    private void getUtilization(String body){
        JSONArray json = null;
        try {
            json = new JSONArray(body);
            var totalMemory = Long.valueOf(json.getJSONObject(0)
                    .getJSONArray("utilization")
                    .getJSONObject(0)
                    .get("total")
                    .toString());
            var usedMemory = Long.valueOf(json.getJSONObject(0)
                    .getJSONArray("utilization")
                    .getJSONObject(0)
                    .get("used")
                    .toString());
            var totalVCore = Long.valueOf(json.getJSONObject(0)
                    .getJSONArray("utilization")
                    .getJSONObject(1)
                    .get("total")
                    .toString());
            var usedVCore = Long.valueOf(json.getJSONObject(0)
                    .getJSONArray("utilization")
                    .getJSONObject(1)
                    .get("used")
                    .toString());
            this.evaluation.setUnused(Long.valueOf(totalMemory-usedMemory).toString(),  Long.valueOf(totalVCore-usedVCore).toString());
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}
