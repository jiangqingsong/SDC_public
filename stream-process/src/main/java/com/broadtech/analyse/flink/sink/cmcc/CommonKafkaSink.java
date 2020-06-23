package com.broadtech.analyse.flink.sink.cmcc;

import com.alibaba.fastjson.JSON;
import com.broadtech.analyse.pojo.cmcc.ResMessage;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-06-10 19:15
 */
public class CommonKafkaSink {
    /**
     *
     * @param env
     * @param producerBrokers broker list path
     * @param producerTopic 发送的topic
     * @param status 返回消息状态 1：success 0：fail
     * @param msg 消息内容
     */
    public static void sink(StreamExecutionEnvironment env, String producerBrokers, String producerTopic, int status, String msg){
        ResMessage resMessage = new ResMessage(1, "data process success!");
        DataStreamSource<String> ok = env.fromElements(JSON.toJSON(resMessage).toString());

        Properties kafkaProp = new Properties();
        kafkaProp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerBrokers);
        kafkaProp.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProp.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringSerializer");

        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>(producerTopic,
                new SimpleStringSchema(), kafkaProp);
        ok.addSink(flinkKafkaProducer);
    }

}
