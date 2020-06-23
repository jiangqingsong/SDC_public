package com.broadtech.analyse.task.cmcc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.broadtech.analyse.constants.asset.AgentCollectConstant;
import com.broadtech.analyse.constants.asset.AssetDiscoverConstant;
import com.broadtech.analyse.constants.asset.AssetFindConstant;
import com.broadtech.analyse.flink.deserialization.CustomJSONDeserializationSchema;
import com.broadtech.analyse.flink.sink.cmcc.discover.AssetDiscoverLoginMysqlSink;
import com.broadtech.analyse.pojo.cmcc.find.*;
import com.broadtech.analyse.util.TimeUtils;
import com.broadtech.analyse.util.env.FlinkUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * @author jiangqingsong
 * @description 资产发现(login)数据入库
 * @date 2020-06-09 10:42
 */
public class AssetDiscoverLogin2MysqlV2 {
    private static Logger LOG = Logger.getLogger(AssetDiscoverLogin2MysqlV2.class);
    private static final OutputTag<ObjectNode> jsonFormatTag = new OutputTag<ObjectNode>("JsonFormat") {
    };
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String propPath = parameterTool.get("conf_path");
        //获取配置数据
        //String propPath = "D:\\SDC\\gitlab_code\\sdcplatform\\SDCPlatform\\stream-process\\src\\main\\resources\\asset_discover_login_cfg.properties";
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

        SingleOutputStreamOperator<List<AssetFindLogin>> json2findLoginStream
                = jsonedAgentStream.map(new Json2FindLogin(producerBrokers, producerTopic))
                //过滤掉非json格式的数据
                .filter(a -> a != null);

        SingleOutputStreamOperator<AssetFindLogin> processAssetFindLonigStream = json2findLoginStream.process(new ProcessFunction<List<AssetFindLogin>, AssetFindLogin>() {
            @Override
            public void processElement(List<AssetFindLogin> findLogins, Context context, Collector<AssetFindLogin> out) throws Exception {
                for (AssetFindLogin assetFindLogin : findLogins) {
                    out.collect(assetFindLogin);
                }
            }
        });

        //开窗批量插入
        processAssetFindLonigStream.timeWindowAll(Time.milliseconds(timeout))
                .apply(new AllWindowFunction<AssetFindLogin, List<AssetFindLogin>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<AssetFindLogin> values, Collector<List<AssetFindLogin>> out) throws Exception {
                        ArrayList<AssetFindLogin> objectNodes = Lists.newArrayList(values);
                        if (objectNodes.size() > 0) {
                            out.collect(objectNodes);
                        }
                    }
                }).addSink(new AssetDiscoverLoginMysqlSink(/*producerBrokers, producerTopic, */jdbcUrl, userName, password));
        env.execute("(login)Asset discover job (java).");
    }

    /**
     * 将Agent原始json字符串转成AgentCollect Pojo
     */
    public static class Json2FindLogin extends RichMapFunction<ObjectNode, List<AssetFindLogin>> {
        private static Logger LOG = Logger.getLogger(Json2FindLogin.class);
        private KafkaProducer<String, String> producer;
        private String broker;
        private String topic;

        public Json2FindLogin(String broker, String topic) {
            this.broker = broker;
            this.topic = topic;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", broker);
            properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
            properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<>(properties);
        }

        @Override
        public List<AssetFindLogin> map(ObjectNode o0) {
            try {
                JSONObject o = JSON.parseObject(o0.toString());
                ArrayList<AssetFindLogin> list = new ArrayList<>();
                String resourceName = o.get(AssetFindConstant.RESOURCE_NAME).toString();
                String taskId = o.get(AssetFindConstant.TASK_ID).toString();
                String scanTime = o.get(AssetFindConstant.SCAN_TIME).toString();
                String isEnd = o.get(AssetFindConstant.IS_END).toString();
                if("true".equals(isEnd)){
                    //返回结束标志
                    long timestamp;
                    try {
                        timestamp = TimeUtils.getTimestamp("yyyy-MM-dd HH:mm:ss", scanTime);
                    }catch (Exception e){
                        timestamp = System.currentTimeMillis();
                    }
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, taskId + "_" + timestamp);
                    producer.send(record);
                }

                JSONArray objects = JSON.parseArray(o.get(AssetFindConstant.RESOURCE_INFO).toString());
                Iterator<Object> iterator = objects.iterator();
                while (iterator.hasNext()){
                    AssetFindLogin assetFindLogin = new AssetFindLogin();
                    String objStr = iterator.next().toString();
                    JSONObject resourceObj = JSON.parseObject(objStr);
                    String switchIP = resourceObj.get(AssetFindConstant.SWITCH_IP).toString();
                    String level = resourceObj.get(AssetFindConstant.LEVEL).toString();

                    String arpJson = resourceObj.get(AssetFindConstant.ARP).toString();
                    List<ARP> arps = JSON.parseArray(arpJson, ARP.class);

                    String neighborJson = resourceObj.get(AssetFindConstant.NEIGHBOR).toString();
                    List<Neighbor> neighbors = JSON.parseArray(neighborJson, Neighbor.class);

                    String ipv4RouteJson = resourceObj.get(AssetFindConstant.Ipv4Route).toString();
                    List<Ipv4Route> ipv4Route = JSON.parseArray(ipv4RouteJson, Ipv4Route.class);

                    String ipv6RouteJson = resourceObj.get(AssetFindConstant.Ipv6Route).toString();
                    List<Ipv6Route> ipv6Route = JSON.parseArray(ipv6RouteJson, Ipv6Route.class);

                    assetFindLogin.setResourceName(resourceName);
                    assetFindLogin.setTaskID(taskId);
                    assetFindLogin.setScanTime(scanTime);
                    assetFindLogin.setSwitchIP(switchIP);
                    assetFindLogin.setLevel(level);
                    assetFindLogin.setArp(arps);
                    assetFindLogin.setNeighbor(neighbors);
                    assetFindLogin.setIpv4Route(ipv4Route);
                    assetFindLogin.setIpv6Route(ipv6Route);
                    list.add(assetFindLogin);
                }
                return list;
            }catch (Exception e){
                LOG.error(e.getMessage(), e);
                return null;
            }
        }
    }

}
