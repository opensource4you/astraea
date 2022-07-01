package org.astraea.app.consumer.experiment;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class Trigger {

    public void killConsumers(ArrayList<Consumer> consumers) throws InterruptedException {
        for(Consumer consumer : consumers) {
            consumer.interrupt();
            TimeUnit.SECONDS.sleep(10);
        }
    }
}
