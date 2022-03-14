package org.astraea.yunikorn.metrics;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.prometheus.client.CollectorRegistry;
import org.astraea.yunikorn.Yunikorn;
import org.astraea.yunikorn.client.Client;
import org.astraea.yunikorn.metrics.Infos.Info;

import java.io.IOException;

public class MetricsExplorer {
    public static void main(String ... argv){
        var args = new Args();
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(argv);
        var info = new Info();
        info.register();
        try {
            Network.exporter();
        } catch (IOException e) {
            e.printStackTrace();
        }
        while(true){
            var network = new  Network();
            info = network.getInfo(args.ip, info);
        }
    }
    private static class Args{
        @Parameter(names = {"-ip"}, description = "Address of yunikorn")
        private String ip = "";
    }
}


