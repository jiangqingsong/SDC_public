package com.broadtech.analyse.task.cmcc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.broadtech.analyse.constants.asset.AgentCollectConstant;
import com.broadtech.analyse.flink.deserialization.CustomJSONDeserializationSchema;
import com.broadtech.analyse.flink.function.cmcc.AgentWithLabelProcessFun;
import com.broadtech.analyse.flink.function.cmcc.AgentWithVulnerabilityProcessFun;
import com.broadtech.analyse.flink.sink.cmcc.AssetAgentWithLabelMysqlSink;
import com.broadtech.analyse.flink.sink.cmcc.AssetAgentWithVulnerMysqlSink;
import com.broadtech.analyse.pojo.cmcc.*;
import com.broadtech.analyse.util.env.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jiangqingsong
 * @description Agent采集数据入mysql【open里面定时获取mysql数据】
 * @date 2020-06-09 10:42
 */
public class AssetAgent2MysqlV2 {
    private static final OutputTag<ObjectNode> jsonFormatTag = new OutputTag<ObjectNode>("JsonFormat") {
    };
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String propPath = parameterTool.get("conf_path");
        //获取配置数据
        //String propPath = "D:\\SDC\\gitlab_code\\sdcplatform\\SDCPlatform\\stream-process\\src\\main\\resources\\asset_login_cfg.properties";
        ParameterTool paramFromProps = ParameterTool.fromPropertiesFile(propPath);
        String consumerTopic = paramFromProps.get("consumer.topic");
        String producerTopic = paramFromProps.get("producer.topic");
        String producerBrokers = paramFromProps.get("producer.bootstrap.server");
        String groupId = paramFromProps.get("consumer.groupId");
        Long timeout = paramFromProps.getLong("timeout");
        //sink mysql config
        String jdbcUrl = paramFromProps.get("jdbcUrl");
        String userName = paramFromProps.get("userName");
        String password = paramFromProps.get("password");
        String sinkLabelTable = paramFromProps.get("sink.label.table");
        String sinkVulnerabilityTable = paramFromProps.get("sink.vulnerability.table");

        final StreamExecutionEnvironment env = FlinkUtils.getEnv();
        env.setParallelism(1);

        //从kafka读取Agent数据
        DataStream<ObjectNode> kafkaStream = FlinkUtils.createKafkaStream(false, paramFromProps, consumerTopic, groupId, CustomJSONDeserializationSchema.class);
        //把json格式的数据放入侧输出流
        DataStream<ObjectNode> jsonedAgentStream = kafkaStream.process(new ProcessFunction<ObjectNode, ObjectNode>() {
            @Override
            public void processElement(ObjectNode value, Context ctx, Collector<ObjectNode> out) throws Exception {
                if (!value.has("jsonParseError")) {
                    ctx.output(jsonFormatTag, value);
                }
            }
        }).getSideOutput(jsonFormatTag);
        //kafkaStream.print();

        SingleOutputStreamOperator<AssetAgentOrigin> json2agentStream
                = jsonedAgentStream.map(new Json2Agent())
                //过滤掉非Agent json格式的数据
                .filter(a -> a != null);

        //关联标签库
        json2agentStream.process(new AgentWithLabelProcessFun(jdbcUrl, userName, password))
                .timeWindowAll(Time.milliseconds(timeout))
                .apply(new AllWindowFunction<Tuple2<AssetAgentOrigin, Tuple3<String, String, String>>, List<Tuple2<AssetAgentOrigin, Tuple3<String, String, String>>>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<AssetAgentOrigin, Tuple3<String, String, String>>> values, Collector<List<Tuple2<AssetAgentOrigin, Tuple3<String, String, String>>>> out) throws Exception {
                        ArrayList<Tuple2<AssetAgentOrigin, Tuple3<String, String, String>>> objectNodes = Lists.newArrayList(values);
                        if (objectNodes.size() > 0) {
                            out.collect(objectNodes);
                        }
                    }
                }).addSink(new AssetAgentWithLabelMysqlSink(sinkLabelTable, jdbcUrl, userName, password));
        //关联漏洞库(agent和login都不做漏洞匹配，但是保留漏洞字段)
        json2agentStream.process(new AgentWithVulnerabilityProcessFun(jdbcUrl, userName, password))
                .timeWindowAll(Time.milliseconds(timeout))
                .apply(new AllWindowFunction<Tuple2<AssetAgentOrigin, String>, List<Tuple2<AssetAgentOrigin, String>>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<AssetAgentOrigin, String>> values, Collector<List<Tuple2<AssetAgentOrigin, String>>> out) throws Exception {
                        ArrayList<Tuple2<AssetAgentOrigin, String>> objectNodes = Lists.newArrayList(values);
                        if (objectNodes.size() > 0) {
                            out.collect(objectNodes);
                        }
                    }
                }).addSink(new AssetAgentWithVulnerMysqlSink(producerBrokers, producerTopic, sinkVulnerabilityTable, jdbcUrl, userName, password));

        String jobname = paramFromProps.get("jobname");
        env.execute(jobname);
    }

    /**
     * 将Agent原始json字符串转成AgentCollect Pojo
     */
    public static class Json2Agent implements MapFunction<ObjectNode, AssetAgentOrigin>{
        private static Logger LOG = Logger.getLogger(Json2Agent.class);
        @Override
        public AssetAgentOrigin map(ObjectNode o0) {

            try {
                JSONObject o = JSON.parseObject(o0.toString());
                AssetAgentOrigin assetAgentOrigin = new AssetAgentOrigin();
                String resourceName = o.get(AgentCollectConstant.RESOURCE_NAME).toString();
                String taskId = o.get(AgentCollectConstant.TASK_ID).toString();
                String assetId = o.get(AgentCollectConstant.ASSET_ID).toString();
                String scanTime = o.get(AgentCollectConstant.SCAN_TIME).toString();

                //resource info
                JSONObject resourceObj = JSON.parseObject(o.get(AgentCollectConstant.RESOURCE_INFO).toString());
                String deviceName = resourceObj.get(AgentCollectConstant.DEVICE_NAME).toString();
                String deviceType = resourceObj.get(AgentCollectConstant.DEVICE_TYPE).toString();
                //deviceIpAddress
                String deviceIpAddressJson = resourceObj.get(AgentCollectConstant.DEVICE_IP_ADDRESS).toString();
                List<String> deviceIpAddress = JSON.parseArray(deviceIpAddressJson, String.class);

                String osInfo = resourceObj.get(AgentCollectConstant.OS_INFO).toString();
                //patchProperties [arr]:
                String patchPropertiesJson = resourceObj.get(AgentCollectConstant.PATCH_PROPERTIES).toString();
                List<String> patchProperties = JSON.parseArray(patchPropertiesJson, String.class);

                String kernelVersion = resourceObj.get(AgentCollectConstant.KERNEL_VERSION).toString();

                //openServiceOfPort [obj]
                String openServiceOfPortJson = resourceObj.get(AgentCollectConstant.OPEN_SERVICE_OF_PORT).toString();
                List<Integer> openServiceOfPorts = JSON.parseArray(openServiceOfPortJson, Integer.class);

                //[obj]
                String programInfoJson = resourceObj.get(AgentCollectConstant.PROGRAM_INFO).toString();
                List<ProgramInfo> programInfos = JSON.parseArray(programInfoJson, ProgramInfo.class);

                //[obj]
                String messageOrientedMiddlewareJson = resourceObj.get(AgentCollectConstant.MESSAGE_ORIENTED_MIDDLEWARE).toString();
                List<MessageOrientedMiddleware> messageOrientedMiddlewares = JSON.parseArray(messageOrientedMiddlewareJson, MessageOrientedMiddleware.class);
                //[obj]
                String dataBaseInfoJson = resourceObj.get(AgentCollectConstant.DATA_BASE_INFO).toString();
                List<DataBaseInfo> dataBaseInfos = JSON.parseArray(dataBaseInfoJson, DataBaseInfo.class);
                assetAgentOrigin.setResourceName(resourceName);
                assetAgentOrigin.setTaskID(taskId);
                assetAgentOrigin.setAssetID(assetId);
                assetAgentOrigin.setScanTime(scanTime);
                assetAgentOrigin.setDeviceName(deviceName);
                assetAgentOrigin.setDeviceType(deviceType);
                assetAgentOrigin.setDeviceIpAddresss(deviceIpAddress);
                assetAgentOrigin.setOSInfo(osInfo);
                assetAgentOrigin.setPatchProperties(patchProperties);
                assetAgentOrigin.setKernelVersion(kernelVersion);
                assetAgentOrigin.setOpenServiceOfPorts(openServiceOfPorts);
                assetAgentOrigin.setProgramInfos(programInfos);
                assetAgentOrigin.setMessageOrientedMiddlewares(messageOrientedMiddlewares);
                assetAgentOrigin.setDataBaseInfos(dataBaseInfos);
                return assetAgentOrigin;
            }catch (Exception e){
                LOG.error(e.getMessage(), e);
                return null;
            }
        }
    }

}
