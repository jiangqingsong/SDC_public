package com.broadtech.analyse.task.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-08-13 17:11
 */
public class TestPro {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.5.92:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0;i < 50;i++){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("test_security_analysis","abc" + i);
            producer.send(producerRecord,(metadata ,e) -> {
                if (e == null){
                    System.out.println("success ->" + metadata.offset());
                }else {
                    e.printStackTrace();
                }
            });
        }
        producer.close();
    }
}
