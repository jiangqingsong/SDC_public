package com.broadtech.analyse.task.main;

import com.broadtech.analyse.flink.deserialization.CustomJSONDeserializationSchema;
import com.broadtech.analyse.flink.process.main.AssetLoadMapFn;
import com.broadtech.analyse.flink.process.main.AssetLoadWithVulnFn;
import com.broadtech.analyse.flink.sink.main.AssetLoad2MysqlSink;
import com.broadtech.analyse.pojo.main.AssetLoadInfo;
import com.broadtech.analyse.pojo.main.VulnUnify;
import com.broadtech.analyse.util.env.FlinkUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author leo.J
 * @description 资产导入碰撞漏洞库
 * @date 2020-09-23 16:05
 */
public class AssetWithVuln {
    private static final OutputTag<ObjectNode> jsonFormatTag = new OutputTag<ObjectNode>("JsonFormat") {
    };
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //String propPath = parameterTool.get("conf_path");
        //获取配置数据
        String propPath = "D:\\SDC\\gitlab_code\\sdcplatform\\SDCPlatform\\stream-process\\src\\main\\resources\\asset_load.properties";
        ParameterTool paramFromProps = ParameterTool.fromPropertiesFile(propPath);
        String consumerTopic = paramFromProps.get("consumer.topic");
        String groupId = paramFromProps.get("consumer.groupId");
        //sink mysql config
        String jdbcUrl = paramFromProps.get("jdbcUrl");
        String userName = paramFromProps.get("userName");
        String password = paramFromProps.get("password");

        final StreamExecutionEnvironment env = FlinkUtils.getEnv();
        env.setParallelism(1);

        //从kafka读取Agent数据
        DataStream<ObjectNode> kafkaStream = FlinkUtils.createKafkaStream(false, paramFromProps, consumerTopic, groupId, CustomJSONDeserializationSchema.class);
        //把json格式的数据放入侧输出流
        DataStream<ObjectNode> jsonedAssetLoadStream = kafkaStream.process(new ProcessFunction<ObjectNode, ObjectNode>() {
            @Override
            public void processElement(ObjectNode value, Context ctx, Collector<ObjectNode> out) throws Exception {
                if (!value.has("jsonParseError")) {
                    ctx.output(jsonFormatTag, value);
                }
            }
        }).getSideOutput(jsonFormatTag);
        SingleOutputStreamOperator<AssetLoadInfo> assetLoadStream = jsonedAssetLoadStream.map(new AssetLoadMapFn());
        SingleOutputStreamOperator<VulnUnify> assetLoadWithVulnStream = assetLoadStream.process(new AssetLoadWithVulnFn(jdbcUrl, userName, password));
        assetLoadWithVulnStream.addSink(new AssetLoad2MysqlSink(jdbcUrl, userName, password));
        env.execute("Asset load job.");

    }
}
