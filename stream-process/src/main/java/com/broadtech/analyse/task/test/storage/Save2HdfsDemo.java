package com.broadtech.analyse.task.test.storage;

import com.broadtech.analyse.flink.deserialization.CustomJSONDeserializationSchema;
import com.broadtech.analyse.util.env.FlinkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author leo.J
 * @description 消费kafka数据，保存到hdfs
 * @date 2020-10-14 11:28
 */
public class Save2HdfsDemo {

    private static Logger LOG = Logger.getLogger(Save2HdfsDemo.class);
    private static final String TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static void main(String[] args) throws Exception {
        //String propPath = "D:\\SDC\\gitlab_code\\sdcplatform\\SDCPlatform\\stream-process\\src\\main\\resources\\storage\\storage_demo.properties";
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String propPath = parameterTool.get("conf_path");
        //获取配置参数
        ParameterTool paramFromProps = ParameterTool.fromPropertiesFile(propPath);
        String consumerTopic = paramFromProps.get("consumer.topic");
        String groupId = paramFromProps.get("consumer.groupId");
        int writeInterval = paramFromProps.getInt("writeInterval");

        //env
        final StreamExecutionEnvironment env = FlinkUtils.getEnv();
        env.setParallelism(1);

        DataStream<String> kafkaSourceStream = FlinkUtils.createKafkaStream(false, paramFromProps, consumerTopic, groupId, SimpleStringSchema.class);

        kafkaSourceStream.print();

        //存储hdfs
        DefaultRollingPolicy rollingPolicy = DefaultRollingPolicy
                .create()
                .withMaxPartSize(1024*1024*128) // 设置每个文件的最大大小 ,默认是128M。这里设置为120M
                .withRolloverInterval(Long.MAX_VALUE) // 滚动写入新文件的时间，默认60s。这里设置为无限大
                .withInactivityInterval(writeInterval*1000) // 60s空闲，就滚动写入新的文件
                .build();

        String hdfsPath = paramFromProps.get("hdfsPath");
        //String hdfsPath = "file:///D:/flinkdemo/data/test";
        StreamingFileSink sink = StreamingFileSink
                .forRowFormat(new Path(hdfsPath), new SimpleStringEncoder<String>("UTF-8"))
                //或者parquet格式存储 .forBulkFormat(new Path("/xxx"), ParquetAvroWriters.forReflectRecord(Xxxx.class))
                /**
                 forRowFormat: 基于行存储
                 forBulkFormat：按列存储，批量编码方式，可以将输出结果用 Parquet 等格式进行压缩存储
                 */
                //.withBucketAssigner(new MemberBucketAssigner())
                .withRollingPolicy(rollingPolicy)
                .withBucketCheckInterval(1000L) // 桶检查间隔，这里设置为1s
                .build();

        kafkaSourceStream.addSink(sink);

        env.execute("sink to hdfs demo.");

    }


}
class MemberBucketAssigner implements BucketAssigner<String, String> {
    @Override
    public String getBucketId(String info, Context context) {
        // 指定桶名 yyyy-mm-dd
        String[] array = info.split(",");
        return StringUtils.join(array, ",");
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
