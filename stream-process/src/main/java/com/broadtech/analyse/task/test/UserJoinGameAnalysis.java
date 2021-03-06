package com.broadtech.analyse.task.test;

import com.broadtech.analyse.util.env.FlinkUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author leo.J
 * @description 统计一天内单个用户参加了几个游戏
 * @date 2020-05-15 15:10
 */
public class UserJoinGameAnalysis {
    private static final Logger LOG = LoggerFactory.getLogger(UserJoinGameAnalysis.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        ParameterTool parameter = null;
        try {
            parameter = ParameterTool.fromPropertiesFile("");
        } catch (IOException e) {
            LOG.error("Config file not exist!");
        }

        DataStream<Object> kafkaStream1 = FlinkUtils.createKafkaStream(true, parameter, "game_analysis", "test1", SimpleStringSchema.class);
        //192.168.86.22020-02-24 20:53:461011504891http://192.168.xxx.xxx:8088/v5.3/gameB.html
        //代表数据之间的分隔符(\001)。如下数据分别代表：IP、参与时间、用户ID、游戏链接
        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(true, parameter, "game_analysis", "test1", SimpleStringSchema.class);

        SingleOutputStreamOperator<String> streamOperator = kafkaStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String log) {
                        String[] split = log.split("\001");
                        String time = split[1];
                        //截取时间(转换为时间戳)
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        LocalDateTime parse = LocalDateTime.parse(time, formatter);
                        return LocalDateTime.from(parse).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                    }
                });

        //Transformation
        SingleOutputStreamOperator<Tuple4<String, String, String, String>> tuple4Operator
                = streamOperator.flatMap((String log, Collector<Tuple4<String, String, String, String>> out) -> {
            String[] logSplit = log.split("\001");
            //访问时间
            String dateTime = logSplit[1];

            String[] dateTimeSplit = dateTime.split(" ");

            //date
            String date = dateTimeSplit[0];
            //time
            String time = dateTimeSplit[1];

            //用户ID(Session)
            String userId = logSplit[2];
            //游戏链接
            String url = logSplit[3];

            out.collect(Tuple4.of(userId, date, time, url));
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING));

        KeyedStream<Tuple4<String, String, String, String>, Tuple> keyedStream = tuple4Operator.keyBy(0, 1);

        /**************************分组后,使用布隆过滤器开始去重(start)***********************/
        SingleOutputStreamOperator<Tuple5<String, String, String, String, Long>> distinctStream = keyedStream.map(new RichMapFunction<Tuple4<String, String, String, String>, Tuple5<String, String, String, String, Long>>() {

            //使用 KeyedState(用于任务失败重启后，从State中恢复数据)
            //1.记录游戏的State
            private transient ValueState<BloomFilter> productState;
            //2.记录次数的State(因为布隆过滤器不会计算它里面到底存了多少数据，所以此处我们创建一个 countState 来计算次数)
            private transient ValueState<Long> countState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //定义一个状态描述器[BloomFilter]
                ValueStateDescriptor<BloomFilter> stateDescriptor = new ValueStateDescriptor<BloomFilter>(
                        "product-state",
                        BloomFilter.class
                );
                //使用RuntimeContext获取状态
                productState = getRuntimeContext().getState(stateDescriptor);

                //定义一个状态描述器[次数]
                ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<Long>(
                        "count-state",
                        Long.class
                );
                //使用RuntimeContext获取状态
                countState = getRuntimeContext().getState(countDescriptor);
            }

            @Override
            public Tuple5<String, String, String, String, Long> map(Tuple4<String, String, String, String> tuple) throws Exception {
                //获取点击链接
                String url = tuple.f3;
                BloomFilter bloomFilter = productState.value();
                if (bloomFilter == null) {
                    //初始化一个bloomFilter
                    bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000);
                    countState.update(0L);
                }
                //BloomFilter 可以判断一定不包含
                if (!bloomFilter.mightContain(url)) {
                    //将当前url加入到bloomFilter
                    bloomFilter.put(url);
                    countState.update(countState.value() + 1);
                }

                //更新 productState
                productState.update(bloomFilter);
                return Tuple5.of(tuple.f0, tuple.f1, tuple.f2, tuple.f3, countState.value());
            }
        });

        /**************************分组后,使用布隆过滤器开始去重(end)***********************/

        distinctStream.print("distinctStream");

        env.execute("UserJoinGameAnalysis");

    }
}
