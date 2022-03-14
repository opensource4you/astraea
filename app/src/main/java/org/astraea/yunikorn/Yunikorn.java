package org.astraea.yunikorn;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.astraea.yunikorn.client.*;

public class Yunikorn {
    public static void main(String ... argv){
        var args = new Args();
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(argv);
        var client = new Client();
        client.getPartitionConfig(args.ip);
        for (;;){
            if(client.listen(args.ip)){
                client.adjust(args.ip);

            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        }
    }
    private static class Args{
        @Parameter(names = {"-ip"}, description = "Address of yunikorn")
        private String ip = "";
    }
}
