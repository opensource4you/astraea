package org.astraea.yunikorn.metrics;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.astraea.yunikorn.Yunikorn;
import org.astraea.yunikorn.client.Client;

public class MetricsExplorer {


    public static void main(String ... argv){
        var args = new Args();
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(argv);
        while(true){
            var network = new  Network();
            var infoCollector = network.getInfo(args.ip);

        }
    }
    private static class Args{
        @Parameter(names = {"-ip"}, description = "Address of yunikorn")
        private String ip = "";
    }
}


