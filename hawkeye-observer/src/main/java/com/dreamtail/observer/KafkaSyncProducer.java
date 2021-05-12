package com.dreamtail.observer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author xdq
 * @version 1.0
 * @className KafkaProducer
 * @description TODO
 * @date 2021/5/12 14:08
 */
public class KafkaSyncProducer {

    public static KafkaProducer<String, String> producer = null;

    public static void initProducer() {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop242:9092,hadoop248:9092,hadoop249:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer = new KafkaProducer<>(prop);
    }

    public static void destroyProducer() {
        producer.close();
    }
}