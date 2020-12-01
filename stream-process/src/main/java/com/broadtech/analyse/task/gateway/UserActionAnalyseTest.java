package com.broadtech.analyse.task.gateway;

import com.broadtech.analyse.flink.process.CountAgg;
import com.broadtech.analyse.flink.process.TopNHotItems;
import com.broadtech.analyse.flink.window.WindowResProcessTe;
import com.broadtech.analyse.pojo.user.ItemViewCount;
import com.broadtech.analyse.pojo.user.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;

/**
 * @author leo.J
 * @description 流量实时统计
 * @date 2020-04-26 15:05
 */
public class UserActionAnalyseTest {
    private static final Logger LOG = LoggerFactory.getLogger(UserActionAnalyseTest.class);
    private final static String KAFKA_BROKERS = "kafka.brokers";
    private final static String KAFKA_TOPIC = "kafka.topic";
    private final static String KAFKA_GROUP_ID = "kafka.groupId";
    private static Map<String, Map<Integer, Tuple2<Double, Double>>> abnormTrafficMap;

    public static void main(String[] args) throws Exception {
        //从args中提取参数
        //ParameterTool fromArgs = ParameterTool.fromArgs(args);

        InputStream inputStream = UserActionAnalyseTest.class.getResourceAsStream("/user_action_cfg.properties");
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

        FlinkKafkaConsumer<String> kafkaConsumer
                = new FlinkKafkaConsumer<>(prop.getProperty(KAFKA_TOPIC), new SimpleStringSchema(), kafkaProp);
        //kafka source
        DataStreamSource<String> kafkaSource = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<UserBehavior> timedStream = kafkaSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                long userId = Long.valueOf(split[0]);
                long itemId = Long.valueOf(split[1]);
                int categoryId = Integer.valueOf(split[2]);
                String behavior = split[3];
                long timestamp = Long.valueOf(split[4]);
                return new UserBehavior(userId, itemId, categoryId, behavior, timestamp);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000;
            }
        });

        DataStream<ItemViewCount> windowedData = timedStream.keyBy(new KeySelector<UserBehavior, Long>() {
            @Override
            public Long getKey(UserBehavior value) throws Exception {
                return value.getItemId();
            }
        }).timeWindow(Time.seconds(10))
                .aggregate(new CountAgg(), new WindowResProcessTe());

        windowedData.print();
        SingleOutputStreamOperator<String> topItems = windowedData.keyBy(new KeySelector<ItemViewCount, Long>() {
            @Override
            public Long getKey(ItemViewCount value) throws Exception {
                return value.getWindowEnd();
            }
        }).process(new TopNHotItems(3));

        topItems.print();
        //execute
        env.execute("traffic  analysis.");
    }
}
