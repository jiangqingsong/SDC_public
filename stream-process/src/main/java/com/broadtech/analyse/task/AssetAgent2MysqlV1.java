package com.broadtech.analyse.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.broadtech.analyse.constants.asset.AgentCollectConstant;
import com.broadtech.analyse.flink.deserialization.CustomJSONDeserializationSchema;
import com.broadtech.analyse.flink.process.cmcc.AgentMatchVulnerabilityProcess;
import com.broadtech.analyse.flink.sink.cmcc.AssetAgentMysqlSink;
import com.broadtech.analyse.flink.source.cmcc.VulnerabilityReader;
import com.broadtech.analyse.pojo.cmcc.*;
import com.broadtech.analyse.pojo.cmcc.find.AssetFindLogin;
import com.broadtech.analyse.util.env.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author leo.J
 * @description Agent采集数据入mysql
 * @date 2020-06-09 10:42
 */
public class AssetAgent2MysqlV1 {
    private static final OutputTag<ObjectNode> jsonFormatTag = new OutputTag<ObjectNode>("JsonFormat") {
    };
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String propPath = parameterTool.get("conf_path");
        //获取配置数据
        //String propPath = "D:\\SDC\\gitlab_code\\sdcplatfor m\\SDCPlatform\\stream-process\\src\\main\\resources\\asset_agent_cfg.properties";
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

        final StreamExecutionEnvironment env = FlinkUtils.getEnv();
        env.setParallelism(1);

        //读取漏洞库数据流并做广播
        DataStreamSource<Map<String, Vulnerability>> vulnerabilitySource = env.addSource(new VulnerabilityReader(jdbcUrl, userName, password));
        //bc stateDesc
        final MapStateDescriptor<Void, Map<String, Vulnerability>> configDescriptor = new MapStateDescriptor(
                "vulnerabilityMap", Types.VOID,
                Types.MAP(Types.STRING, Types.POJO(Vulnerability.class)));
        BroadcastStream<Map<String, Vulnerability>> vulnerBcStream = vulnerabilitySource.broadcast(configDescriptor);
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

        SingleOutputStreamOperator<AssetAgentOrigin> json2agentStream
                = jsonedAgentStream.map(new Json2Agent())
                //过滤掉非Agent json格式的数据
                .filter(a -> a != null);

        BroadcastConnectedStream<AssetAgentOrigin, Map<String, Vulnerability>> connectedStream
                = json2agentStream.connect(vulnerBcStream);

        SingleOutputStreamOperator<Tuple2<AssetAgentOrigin, String>> matchedVulnerabilityStream
                = connectedStream.process(new AgentMatchVulnerabilityProcess(configDescriptor));

        //开窗收集数据做批量插入
        DataStreamSink<List<Tuple2<AssetAgentOrigin, String>>> listDataStreamSink = matchedVulnerabilityStream.timeWindowAll(Time.milliseconds(timeout))
                .apply(new AllWindowFunction<Tuple2<AssetAgentOrigin, String>, List<Tuple2<AssetAgentOrigin, String>>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<AssetAgentOrigin, String>> values, Collector<List<Tuple2<AssetAgentOrigin, String>>> out) throws Exception {
                        ArrayList<Tuple2<AssetAgentOrigin, String>> objectNodes = Lists.newArrayList(values);
                        if (objectNodes.size() > 0) {
                            out.collect(objectNodes);
                        }
                    }
                }).addSink(new AssetAgentMysqlSink(jdbcUrl, userName, password));
        //todo kafka上报数据处理成功消息,需要在每次入库完成之后上报状态
        //CommonKafkaSink.sink(env, producerBrokers, producerTopic, 1, "data process is ok.");
        env.execute("Agent collect job (java).");
    }

    /**
     * 将Agent原始json字符串转成AgentCollect Pojo
     */
    public static class Json2Agent implements MapFunction<ObjectNode, AssetAgentOrigin>{
        private String separator = ",";
        @Override
        public AssetAgentOrigin map(ObjectNode o0) throws Exception {

            try {
                JSONObject o = JSON.parseObject(o0.toString());
                ArrayList<AssetFindLogin> list = new ArrayList<>();
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

                //arr
                /*String startUpJson = resourceObj.get(AgentCollectConstant.START_UP).toString();
                List<String> startUps = JSON.parseArray(startUpJson, String.class);*/
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
                e.printStackTrace();
                return null;
            }
        }
    }

}
