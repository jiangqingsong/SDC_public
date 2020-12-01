package com.broadtech.analyse.task.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author leo.J
 * @description
 * @date 2020-10-22 17:38
 */
public class KafkaProducerTest {
    public static void main(String[] args) {
        writeToKafka("test_kafka_001");
    }
    public static void writeToKafka(String topic) {
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers", "localhost:19092");
        properties.setProperty("bootstrap.servers", "slave02:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //定义一个kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String testContent = "testtesttesttesttesttesttesttesttesttesttesttest";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, testContent);
        producer.send(record);
        producer.close();
    }

}
