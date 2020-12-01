package com.broadtech.analyse.task;

import com.broadtech.analyse.flink.deserialization.CustomJSONDeserializationSchema;
import com.broadtech.analyse.flink.sink.cmcc.AssetDiscoverMysqlSink;
import com.broadtech.analyse.flink.trigger.CountTriggerWithTimeout;
import com.broadtech.analyse.util.env.FlinkUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

/**
 * @author leo.J
 * @description  资产发现数据kafka2mysql
 * @date 2020-05-28 13:54
 */
public class AssetDiscover2MysqlV1 {

    private static final OutputTag<ObjectNode> jsonFormatTag = new OutputTag<ObjectNode>("JsonFormat") {
    };
    public static void main(String[] args) throws Exception {

        //parameter
        /*ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String propPath = parameterTool.get("conf_path");*/
        String propPath = "D:\\SDC\\gitlab_code\\sdcplatform\\SDCPlatform\\stream-process\\src\\main\\resources\\asset_discover_cfg.properties";
        ParameterTool paramFromProps = ParameterTool.fromPropertiesFile(propPath);
        String topic = paramFromProps.get("kafka.topic");
        String groupId = paramFromProps.get("kafka.groupId");
        int maxCount = paramFromProps.getInt("maxCount");
        Long timeout = paramFromProps.getLong("timeout");

        final StreamExecutionEnvironment env = FlinkUtils.getEnv();
        env.setParallelism(1);//本地测试用并行度1
        DataStream<ObjectNode> kafkaStream = FlinkUtils.createKafkaStream(true, paramFromProps, topic, groupId, CustomJSONDeserializationSchema.class);

        /**
         *  timeOut秒或者超过指定maxCount数触发一次批量插入
         */
        //time + count
        SingleOutputStreamOperator<ObjectNode> tagOutputStream = kafkaStream.process(new ProcessFunction<ObjectNode, ObjectNode>() {
            @Override
            public void processElement(ObjectNode value, Context ctx, Collector<ObjectNode> out) throws Exception {
                if (!value.has("jsonParseError")) {
                    ctx.output(jsonFormatTag, value);
                }
            }
        });
        tagOutputStream.getSideOutput(jsonFormatTag)
                .timeWindowAll(Time.milliseconds(timeout))
                .trigger(new CountTriggerWithTimeout<ObjectNode>(maxCount, TimeCharacteristic.ProcessingTime))
                .apply(new AllWindowFunction<ObjectNode, List<ObjectNode>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<ObjectNode> values, Collector<List<ObjectNode>> out) throws Exception {
                        ArrayList<ObjectNode> objectNodes = Lists.newArrayList(values);
                        if(objectNodes.size() > 0){
                            out.collect(objectNodes);
                        }
                    }
                }).addSink(new AssetDiscoverMysqlSink());
        env.execute("asset discover job");
    }
}
