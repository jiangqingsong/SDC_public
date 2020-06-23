package com.broadtech.analyse.task.gateway;

import com.broadtech.analyse.flink.sink.DpiMysqlSink;
import com.broadtech.analyse.flink.sink.MySqlTwoPhaseCommitSink;
import com.broadtech.analyse.flink.trigger.CountTriggerWithTimeout;
import com.broadtech.analyse.util.env.FlinkUtils;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.omg.CORBA.Object;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author jiangqingsong
 * @description  1、从kafka读取DPI数据  2、sink到mysql
 * @date 2020-05-28 13:54
 */
public class DPI2Mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        env.setParallelism(1);
        //checkpoint的设置
        //每隔10s进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(10000);
        //设置模式为：exactly_one，仅一次语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //同一时间只允许进行一次检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //设置kafka消费参数
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master01:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dpi-consumer-01");
        //kafka分区自动发现周期
        props.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "3000");


        FlinkKafkaConsumer<ObjectNode> kafkaConsumer = new FlinkKafkaConsumer<>("asset_dpi",
                new JSONKeyValueDeserializationSchema(true), props);
        kafkaConsumer.setStartFromLatest();
        DataStreamSource<ObjectNode> kafkaStream = env.addSource(kafkaConsumer);
        //kafkaStream.print();
        /**
         * strategy 1
         */
        //strategy 1: kafkaStream.addSink(new MySqlTwoPhaseCommitSink());
        /**
         * strategy 2: 5秒内数据批量插入
         */
        /*kafkaStream.timeWindowAll(Time.milliseconds(5000)).apply(new AllWindowFunction<ObjectNode, List<ObjectNode>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<ObjectNode> values, Collector<List<ObjectNode>> out) throws Exception {
                ArrayList<ObjectNode> objectNodes = Lists.newArrayList(values);
                if(objectNodes.size() > 0){
                    out.collect(objectNodes);
                }
            }
        }).addSink(new DpiMysqlSink());*/

        /**
         * strategy 3: 5秒或者超过指定maxCount数触发一次批量插入
         */
        //time + count
        int maxCount = 6000;
        kafkaStream.timeWindowAll(Time.milliseconds(5000))
                .trigger(new CountTriggerWithTimeout<ObjectNode>(maxCount, TimeCharacteristic.ProcessingTime))
                .apply(new AllWindowFunction<ObjectNode, List<ObjectNode>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<ObjectNode> values, Collector<List<ObjectNode>> out) throws Exception {
                        ArrayList<ObjectNode> objectNodes = Lists.newArrayList(values);
                        if(objectNodes.size() > 0){
                            out.collect(objectNodes);
                        }
                    }
                }).addSink(new DpiMysqlSink());
        env.execute("kafka2mysql job");
    }
}
