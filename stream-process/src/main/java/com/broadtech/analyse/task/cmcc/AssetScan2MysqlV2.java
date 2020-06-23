package com.broadtech.analyse.task.cmcc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.broadtech.analyse.constants.asset.ScanCollectConstant;
import com.broadtech.analyse.flink.deserialization.CustomJSONDeserializationSchema;
import com.broadtech.analyse.flink.function.cmcc.ScanWithLabelProcessFun;
import com.broadtech.analyse.flink.function.cmcc.ScanWithVulnerabilityProcessFun;
import com.broadtech.analyse.flink.sink.cmcc.AssetScanWithLabelMysqlSink;
import com.broadtech.analyse.flink.sink.cmcc.AssetScanWithVulnerMysqlSink;
import com.broadtech.analyse.pojo.cmcc.*;
import com.broadtech.analyse.util.czip.IpLocation;
import com.broadtech.analyse.util.czip.Location;
import com.broadtech.analyse.util.env.FlinkUtils;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author jiangqingsong
 * @description 远程采集数据入mysql
 * @date 2020-06-09 10:42
 */
public class AssetScan2MysqlV2 {
    private static Logger LOG = Logger.getLogger(AssetScan2MysqlV2.class);
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String propPath = parameterTool.get("conf_path");
        //获取配置数据
        //String propPath = "D:\\SDC\\gitlab_code\\sdcplatform\\SDCPlatform\\stream-process\\src\\main\\resources\\asset_scan_cfg.properties";
        ParameterTool paramFromProps = ParameterTool.fromPropertiesFile(propPath);
        String consumerTopic = paramFromProps.get("consumer.topic");
        String completedTopic = paramFromProps.get("producer.completed.topic");
        String discoverTopic = paramFromProps.get("producer.discover.topic");
        String producerBrokers = paramFromProps.get("producer.bootstrap.server");
        String groupId = paramFromProps.get("consumer.groupId");
        Long timeout = paramFromProps.getLong("timeout");
        //sink mysql config
        String jdbcUrl = paramFromProps.get("jdbcUrl");
        String userName = paramFromProps.get("userName");
        String password = paramFromProps.get("password");
        //ip数据库地址
        String locationPath = paramFromProps.get("locationPath");

        final StreamExecutionEnvironment env = FlinkUtils.getEnv();
        env.setParallelism(1);

        //从kafka读取Agent数据
        DataStream<ObjectNode> kafkaStream = FlinkUtils.createKafkaStream(false, paramFromProps, consumerTopic, groupId, CustomJSONDeserializationSchema.class);
        //把json格式的数据放入侧输出流
        final OutputTag<ObjectNode> jsonFormatTag = new OutputTag<ObjectNode>("JsonFormat") {
        };
        DataStream<ObjectNode> jsonedAgentStream = kafkaStream.process(new ProcessFunction<ObjectNode, ObjectNode>() {
            @Override
            public void processElement(ObjectNode value, Context ctx, Collector<ObjectNode> out) throws Exception {
                if (!value.has("jsonParseError")) {
                    ctx.output(jsonFormatTag, value);
                }
            }
        }).getSideOutput(jsonFormatTag);

        SingleOutputStreamOperator<AssetScanOrigin> json2agentStream
                = jsonedAgentStream.keyBy(x -> "1").process(new Json2ScanProcess(producerBrokers, completedTopic, discoverTopic, locationPath));

        //关联标签库
        SingleOutputStreamOperator<Tuple2<AssetScanOrigin, Tuple3<String, String, String>>> labelProcessedStream
                = json2agentStream.process(new ScanWithLabelProcessFun(producerBrokers, discoverTopic, jdbcUrl, userName, password));
        labelProcessedStream
                .timeWindowAll(Time.milliseconds(timeout))
                .apply(new AllWindowFunction<Tuple2<AssetScanOrigin, Tuple3<String, String, String>>, List<Tuple2<AssetScanOrigin, Tuple3<String, String, String>>>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<AssetScanOrigin, Tuple3<String, String, String>>> values, Collector<List<Tuple2<AssetScanOrigin, Tuple3<String, String, String>>>> out) throws Exception {
                        ArrayList<Tuple2<AssetScanOrigin, Tuple3<String, String, String>>> objectNodes = Lists.newArrayList(values);
                        if (objectNodes.size() > 0) {
                            out.collect(objectNodes);
                        }
                    }
                }).addSink(new AssetScanWithLabelMysqlSink(jdbcUrl, userName, password));

        //关联漏洞库
        json2agentStream.process(new ScanWithVulnerabilityProcessFun(jdbcUrl, userName, password))
                .timeWindowAll(Time.milliseconds(timeout))
                .apply(new AllWindowFunction<Tuple2<AssetScanOrigin, String>, List<Tuple2<AssetScanOrigin, String>>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<AssetScanOrigin, String>> values, Collector<List<Tuple2<AssetScanOrigin, String>>> out) throws Exception {
                        ArrayList<Tuple2<AssetScanOrigin, String>> objectNodes = Lists.newArrayList(values);
                        if (objectNodes.size() > 0) {
                            out.collect(objectNodes);
                        }
                    }
                }).addSink(new AssetScanWithVulnerMysqlSink(jdbcUrl, userName, password));

        env.execute("Scan collect job (java).");
    }


    /**
     * json转Scan对象
     */
    public static class Json2ScanProcess extends ProcessFunction<ObjectNode, AssetScanOrigin>{
        private transient ValueState<Map<String, Integer>> completedCountMapState;
        private static Logger LOG = Logger.getLogger(AssetScan2MysqlV2.class);
        private String broker;
        private String topic;
        private String isEndTopic;
        private KafkaProducer<String, String> producer;
        private String locationPath;

        private IpLocation ipLocation;
        public Json2ScanProcess(String broker, String topic, String isEndTopic, String locationPath) {
            this.broker = broker;
            this.topic = topic;
            this.isEndTopic = isEndTopic;
            this.locationPath = locationPath;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor descriptor = new ValueStateDescriptor<>("completedCountMapState", Types.MAP(Types.STRING, Types.INT));
            completedCountMapState = getRuntimeContext().getState(descriptor);

            super.open(parameters);
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", broker);
            properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
            properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<>(properties);

            //ip归属
            ipLocation = new IpLocation(locationPath);
        }

        @Override
        public void processElement(ObjectNode o0, Context context, Collector<AssetScanOrigin> out) {
            try {
                JSONObject o = JSON.parseObject(o0.toString());
                String resourceName = o.get(ScanCollectConstant.RESOURCE_NAME).toString();
                String taskId = o.get(ScanCollectConstant.TASK_ID).toString();
                String scanTime = o.get(ScanCollectConstant.SCAN_TIME).toString();
                //resource info
                JSONObject resourceObj = JSON.parseObject(o.get(ScanCollectConstant.RESOURCE_INFO).toString());


                if(resourceObj.size() == 0){
                    //下发login资产发现结束标志
                    if(ScanCollectConstant.RESOURCE_NAME_TAG.equals(resourceName)){
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("TaskID", taskId);
                        jsonObject.put("IPAddress", "");
                        jsonObject.put("IsEnd", "true");
                        ProducerRecord<String, String> record = new ProducerRecord<>(isEndTopic, jsonObject.toJSONString());
                        producer.send(record);
                    }
                    int count = 1;
                    Map<String, Integer> countMap = completedCountMapState.value();
                    if(countMap == null){
                        countMap = new HashMap<>();
                    }
                    if(countMap.containsKey(taskId)){
                        count = countMap.get(taskId) + 1;
                    }
                    countMap.put(taskId, count);
                    completedCountMapState.update(countMap);
                    if(count == 3){
                        //send kafka
                        LOG.info(taskId + "扫描完成!");
                        //send kafka message
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, taskId + "_" + System.currentTimeMillis());
                        producer.send(record);
                        //清楚该taskid的状态
                        Map<String, Integer> map = completedCountMapState.value();
                        map.remove(taskId);
                        completedCountMapState.update(map);
                    }
                }else {
                    //deviceIpAddress
                    String deviceIpAddress = resourceObj.get(ScanCollectConstant.DEVICE_IP_ADDRESS).toString();
                    String ipAddressOwnership = resourceObj.get(ScanCollectConstant.IP_ADDRESS_OWNERSHIP).toString();
                    String deviceType = resourceObj.get(ScanCollectConstant.DEVICE_TYPE).toString();
                    String osInfo = resourceObj.get(ScanCollectConstant.OS_INFO).toString();
                    String systemFingerprintInfo = resourceObj.get(ScanCollectConstant.SYSTEM_FINGERPRINT_INFO).toString();
                    //openServiceOfPort [obj]
                    String openServiceOfPortJson = resourceObj.get(ScanCollectConstant.OPEN_SERVICE_OF_PORT).toString();
                    List<OpenServiceOfPort> openServiceOfPorts = JSON.parseArray(openServiceOfPortJson, OpenServiceOfPort.class);
                    //[obj]
                    String messageOrientedMiddlewareJson = resourceObj.get(ScanCollectConstant.MESSAGE_ORIENTED_MIDDLEWARE).toString();
                    List<MessageOrientedMiddlewareScan> messageOrientedMiddlewares = JSON.parseArray(messageOrientedMiddlewareJson, MessageOrientedMiddlewareScan.class);
                    //[obj]
                    String dataBaseInfoJson = resourceObj.get(ScanCollectConstant.DATA_BASE_INFO).toString();
                    List<DataBaseInfoScan> dataBaseInfos = JSON.parseArray(dataBaseInfoJson, DataBaseInfoScan.class);
                    String running = resourceObj.get(ScanCollectConstant.RUNNING).toString();

                    String province = "";
                    //ip归属省份
                    try {
                        Location location = ipLocation.fetchIPLocation(deviceIpAddress.trim());
                        province = location.country;
                    }catch (Exception e){
                        LOG.error(e.getMessage(), e);
                    }
                    AssetScanOrigin scan = new AssetScanOrigin();
                    scan.setResourceName(resourceName);
                    scan.setTaskID(taskId);
                    scan.setScanTime(scanTime);
                    scan.setDeviceIPAddress(deviceIpAddress);
                    //scan.setIPAddressOwnership(ipAddressOwnership);
                    scan.setIPAddressOwnership("".equals(province) ? "中国" : province);
                    scan.setDeviceType(deviceType);
                    scan.setOSInfo(osInfo);
                    scan.setSystemFingerprintInfo(systemFingerprintInfo);
                    scan.setOpenServiceOfPort(openServiceOfPorts);
                    scan.setMessageOrientedMiddleware(messageOrientedMiddlewares);
                    scan.setDataBaseInfos(dataBaseInfos);
                    scan.setRuning(running);
                    out.collect(scan);
                }
            }catch (Exception e){
                LOG.error(e.getMessage(), e);
            }
        }
    }
}
