package com.broadtech.analyse.task.gateway;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-05-14 10:00
 */
public class TempStateTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("master01", 7777);

        SingleOutputStreamOperator<Tuple3> mapedStream = stream.map(x -> x.split(",")).filter(s -> s.length == 3)
                .map(a -> new Tuple3<Long, Long, Double>(Long.valueOf(a[0]), Long.valueOf(a[1]), Double.valueOf(a[2])));

        mapedStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple3 element) {
                return (long)element.f1 * 1000;
            }
        });


    }
}
