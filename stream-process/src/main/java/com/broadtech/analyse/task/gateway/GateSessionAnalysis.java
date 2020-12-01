package com.broadtech.analyse.task.gateway;

import com.broadtech.analyse.flink.function.GatewayAppTypeTrafficAggFn;
import com.broadtech.analyse.flink.function.GatewayFilterFn;
import com.broadtech.analyse.flink.function.GatewayTypeMapFn;
import com.broadtech.analyse.flink.process.AppTypeRateProcessFn;
import com.broadtech.analyse.flink.watermark.GatewayAssignTimestampAndWatermarks;
import com.broadtech.analyse.flink.window.GatewayAppTypeResProcessFn;
import com.broadtech.analyse.pojo.gateway.GatewayAppTypeView;
import com.broadtech.analyse.pojo.gateway.GatewaySession;
import com.broadtech.analyse.task.traffic.TrafficAnalyse;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;

/**
 * @author leo.J
 * @description 网关数据分析：
 * 1、用户行为分析
 * 2、资产画像
 * 3、儿童网络防护
 * 4、风险访问态势
 * @date 2020-05-09 15:38
 */
public class GateSessionAnalysis {
    private static final Logger LOG = LoggerFactory.getLogger(GateSessionAnalysis.class);
    private final static String KAFKA_BROKERS = "kafka.brokers";
    private final static String KAFKA_TOPIC = "kafka.topic";
    private final static String KAFKA_GROUP_ID = "kafka.groupId";

    public static void main(String[] args) throws Exception {
        InputStream inputStream = TrafficAnalyse.class.getResourceAsStream("/asset_discover_login_cfg.properties");

        Properties prop = new Properties();
        try {
            prop.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();//日志输出
        }
        Properties kafkaProp = new Properties();
        //kafka consumer
        kafkaProp.setProperty("bootstrap.servers", prop.getProperty(KAFKA_BROKERS));
        //kafkaProp.setProperty("group.id", prop.getProperty(KAFKA_GROUP_ID));
        kafkaProp.setProperty("fetch.min.bytes", "100000");
        kafkaProp.setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, (24 * 60 * 10000) + "");
        kafkaProp.setProperty("auto.offset.reset", "latest");

        //get env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //set checkpoint
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置statebackend
        //env.setStateBackend(new RocksDBStateBackend("xxx",true));

        FlinkKafkaConsumer<String> kafkaConsumer
                = new FlinkKafkaConsumer<>(prop.getProperty(KAFKA_TOPIC), new SimpleStringSchema(), kafkaProp);
        //kafka source
        DataStreamSource<String> kafkaSource = env.addSource(kafkaConsumer);

        GatewayFilterFn gatewayFilterFn = new GatewayFilterFn();
        gatewayFilterFn.setSeparator("|");

        SingleOutputStreamOperator<GatewaySession> formatedStream
                = kafkaSource.filter(gatewayFilterFn)
                .map(new GatewayTypeMapFn())
                .assignTimestampsAndWatermarks(new GatewayAssignTimestampAndWatermarks(Time.minutes(1)));

        //1、用户行为分析
        SingleOutputStreamOperator<GatewayAppTypeView> appTypeWindowedStream = formatedStream.keyBy(new KeySelector<GatewaySession, String>() {
            @Override
            public String getKey(GatewaySession value) throws Exception {
                //根据应用大类keyBy
                return value.getAppType();
            }
        }).window(TumblingEventTimeWindows.of(Time.days(1L), Time.hours(-8)))
                .trigger(ContinuousEventTimeTrigger.of(Time.milliseconds(5 * 60 * 1000L)))
                .aggregate(new GatewayAppTypeTrafficAggFn(), new GatewayAppTypeResProcessFn());

        appTypeWindowedStream.timeWindowAll(Time.minutes(1));
        appTypeWindowedStream.keyBy(new KeySelector<GatewayAppTypeView, Long>() {
            @Override
            public Long getKey(GatewayAppTypeView value) throws Exception {
                return value.getWindowEnd();//以一天为window，后面process可以收集到一天总的流量
            }
        }).process(new AppTypeRateProcessFn());

        env.execute("Gateway session analysis.");
    }
}
