package com.broadtech.analyse.task.traffic;

import com.broadtech.analyse.flink.function.TrafficAggregateFunction;
import com.broadtech.analyse.flink.process.AbnormaProcessFunc;
import com.broadtech.analyse.flink.process.TrafficProcessFunction;
import com.broadtech.analyse.flink.watermark.TrafficAssignTimestampAndWatermarks;
import com.broadtech.analyse.pojo.traffic.Traffic;
import com.broadtech.analyse.flink.source.JdbcReader;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;

/**
 * @author jiangqingsong
 * @description 流量实时统计
 * @date 2020-04-26 15:05
 */
public class TrafficAnalyse {
    private static final Logger LOG = LoggerFactory.getLogger(TrafficAnalyse.class);
    private final static String KAFKA_BROKERS = "kafka.brokers";
    private final static String KAFKA_TOPIC = "kafka.topic";
    private final static String KAFKA_GROUP_ID = "kafka.groupId";
    private static Map<String, Map<Integer, Tuple2<Double, Double>>> abnormTrafficMap;

    public static void main(String[] args) throws Exception {
        //从args中提取参数
        //ParameterTool fromArgs = ParameterTool.fromArgs(args);

        InputStream inputStream = TrafficAnalyse.class.getResourceAsStream("/cfg.properties");

        Properties prop = new Properties();
        try {
            prop.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();//日志输出
        }
        Properties kafkaProp = new Properties();
        //kafka consumer
        kafkaProp.setProperty("bootstrap.servers", prop.getProperty(KAFKA_BROKERS));
        kafkaProp.setProperty("group.id", prop.getProperty(KAFKA_GROUP_ID));
        kafkaProp.setProperty("fetch.min.bytes", "100000");
        kafkaProp.setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, (24 * 60 * 10000) + "");
        //kafkaProp.setProperty("auto.offset.reset", "earliest");
        kafkaProp.setProperty("auto.offset.reset", "earliest");


        //get env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer<String> kafkaConsumer
                = new FlinkKafkaConsumer<>(prop.getProperty(KAFKA_TOPIC), new SimpleStringSchema(), kafkaProp);
        //kafka source
        DataStreamSource<String> kafkaSource = env.addSource(kafkaConsumer);
        kafkaSource.print();

        //获取异常流量上下限数据
        DataStreamSource<Map<Integer, Tuple2<Double, Double>>> configStream
                = env.addSource(new JdbcReader());
        //configStream.print();
        //将上下界广播
        MapStateDescriptor configDescriptor = new MapStateDescriptor(
                "abnormalBound", Types.VOID, Types.MAP(Types.INT, Types.TUPLE(Types.DOUBLE, Types.DOUBLE)));
        BroadcastStream<Map<Integer, Tuple2<Double, Double>>> broadcastConfigStream
                = configStream.broadcast(configDescriptor);
        //transfer && connect
        BroadcastConnectedStream<Tuple2<String, Double>, Map<Integer, Tuple2<Double, Double>>> connectedStream
                = kafkaSource.map(new MapFunction<String, String[]>() {
            @Override
            public String[] map(String value) throws Exception {
                return value.split(",");
            }
        }).filter(new FilterFunction<String[]>() {
            @Override
            public boolean filter(String[] value) throws Exception {
                return value.length == 13;
            }
        }).map(new MapFunction<String[], Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String[] value) throws Exception {
                String startTime = value[8];
                Double utraffic = Double.valueOf(value[9]);
                Double dtraffic = Double.valueOf(value[10]);
                return new Tuple2(startTime, utraffic + dtraffic);
            }
        }).connect(broadcastConfigStream);

        SingleOutputStreamOperator<Traffic> processedTrafficStream
                = connectedStream.process(new AbnormaProcessFunc());

        SingleOutputStreamOperator<String> aggregatedStream = processedTrafficStream
                .assignTimestampsAndWatermarks(new TrafficAssignTimestampAndWatermarks(Time.minutes(0)))
                .map(x -> {
                    x.setKey("1");
                    return x;
                })
                .keyBy(x -> x.getKey())
                //window大小为天的话存在时区问题
                .window(TumblingEventTimeWindows.of(Time.days(1L), Time.hours(-8)))
                //5分钟触发一次
                .trigger(ContinuousEventTimeTrigger.of(Time.milliseconds(5 /** 60 */* 1000L)))
                .aggregate(new TrafficAggregateFunction(), new TrafficProcessFunction());
        //sink
        aggregatedStream.print();

        //execute
        env.execute("abnormal traffic analysis");
    }
}
