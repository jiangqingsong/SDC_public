package com.broadtech.analyse.task.test.storage;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.broadtech.analyse.util.env.FlinkUtils;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.time.ZoneId;
import java.util.Properties;

/**
 * @author leo.J
 * @description Consumer kafka topic & convert data to parquet.
 * @date 2020-10-29 10:20
 */
public class FlinkParquetUtils {
    private final static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    static {
        /** Set flink env info. */
        env.enableCheckpointing(60 * 1000);
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }

    /** Consumer topic data && parse to hdfs. */
    public static void getTopicToHdfsByParquet(StreamExecutionEnvironment env) {
        try {
            String propPath = "D:\\SDC\\gitlab_code\\sdcplatform\\SDCPlatform\\stream-process\\src\\main\\resources\\storage\\flink_parquet.properties";
            ParameterTool paramFromProps = ParameterTool.fromPropertiesFile(propPath);
            String topic = paramFromProps.get("kafka.topic");
            String groupId = paramFromProps.get("group.id");
            String path = paramFromProps.get("hdfs.path");
            String pathFormat = paramFromProps.get("hdfs.path.date.format");
            String zone = paramFromProps.get("hdfs.path.date.zone");
            Long windowTime = Long.valueOf(paramFromProps.get("window.time.second"));
            DataStream<String> kafkaSourceStream = FlinkUtils.createKafkaStream(false, paramFromProps, topic, groupId, SimpleStringSchema.class);

            final StreamingFileSink<Object> streamingFileSink = StreamingFileSink
                    .forRowFormat(new Path(path), new SimpleStringEncoder<>("UTF-8"))
                    .build();
            /*kafkaSourceStream.addSink(streamingFileSink).name("Sink To HDFS");
            env.execute("sink to hdfs by parquet job.");*/
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    /**
     * transform origin data to TopicSource
     * @param data
     * @return
     */
    private static TopicSource transformData(String data) {
        if (data != null && !data.isEmpty()) {
            JSONObject value = JSON.parseObject(data);
            TopicSource topic = new TopicSource();
            topic.setId(value.getString("id"));
            topic.setTime(value.getLong("time"));
            return topic;
        } else {
            return new TopicSource();
        }
    }
    private static class CustomWatermarks<T> implements AssignerWithPunctuatedWatermarks<TopicSource> {

        /**
         *
         */
        private static final long serialVersionUID = 1L;
        private Long cuurentTime = 0L;

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(TopicSource topic, long l) {
            return new Watermark(cuurentTime);
        }

        @Override
        public long extractTimestamp(TopicSource topic, long l) {
            Long time = topic.getTime();
            cuurentTime = Math.max(time, cuurentTime);
            return time;
        }
    }


    public static void main(String[] args) {
        getTopicToHdfsByParquet(env);
    }


}
