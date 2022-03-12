package org.astraea.yunikorn.metrics;

import org.astraea.yunikorn.metrics.Infos.Info;
import org.astraea.yunikorn.metrics.Infos.Node;
import org.json.JSONArray;
import org.json.JSONException;
import com.google.gson.*;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class Network {
    public InfoCollector getInfo(String ip) {
        HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(20))
                .build();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + ip + "/ws/v1/nodes"))
                .GET()
                .build();
        var infoCollector = new InfoCollector();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            var body = response.body();
            var nodes = getNodes(body);
            var info = new Info(nodes);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return infoCollector;
    }

    private List<Node> getNodes(String body){
        List<Node> nodes = new ArrayList<>();
        try {
            var json = new JSONArray(body);
            var gson = new Gson();
            for (int i = 0; i < json.length();i++){
                var node = gson.fromJson(json.getJSONObject(i).toString(), Node.class);
                nodes.add(node);
            }

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return nodes;
    }



}
