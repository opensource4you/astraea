package org.astraea.performance;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;

public class WarmUp {
    public static void warmUp(ComponentFactory componentFactory, String brokers, String topic, Integer partition){
        byte[] payload = new byte[1];
        Properties prop = new Properties();
        prop.put("bootstrap.servers", brokers);

        // Create a kafkaProducer
        /*try (Producer producer = componentFactory.createProducer()){
            for (int i = 0; i<partition; ++i){
                producer.send(new ProducerRecord<>(topic, i, System.currentTimeMillis(), null, payload));
            }
        }*/
    }
}
