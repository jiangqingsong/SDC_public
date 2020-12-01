package com.broadtech.analyse.task.cmcc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.broadtech.analyse.constants.asset.AssetFindConstant;
import com.broadtech.analyse.flink.deserialization.CustomJSONDeserializationSchema;
import com.broadtech.analyse.flink.sink.cmcc.discover.AssetDiscoverLoginMysqlSink;
import com.broadtech.analyse.flink.sink.cmcc.discover.AssetDiscoverScanMysqlSink;
import com.broadtech.analyse.pojo.cmcc.find.*;
import com.broadtech.analyse.util.env.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
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
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author leo.J
 * @description 资产发现(scan)数据入库
 * @date 2020-06-09 10:42
 */
public class AssetDiscoverScan2MysqlV2 {
    private static final OutputTag<ObjectNode> jsonFormatTag = new OutputTag<ObjectNode>("JsonFormat") {
    };
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //String propPath = parameterTool.get("conf_path");
        //获取配置数据
        String propPath = "D:\\SDC\\gitlab_code\\sdcplatform\\SDCPlatform\\stream-process\\src\\main\\resources\\asset_discover_scan_cfg.properties";
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
        DataStream<ObjectNode> kafkaStream = FlinkUtils.createKafkaStream(true, paramFromProps, consumerTopic, groupId, CustomJSONDeserializationSchema.class);
        //把json格式的数据放入侧输出流
        DataStream<ObjectNode> jsonedAgentStream = kafkaStream.process(new ProcessFunction<ObjectNode, ObjectNode>() {
            @Override
            public void processElement(ObjectNode value, Context ctx, Collector<ObjectNode> out) throws Exception {
                if (!value.has("jsonParseError")) {
                    ctx.output(jsonFormatTag, value);
                }
            }
        }).getSideOutput(jsonFormatTag);

        SingleOutputStreamOperator<List<AssetFindScan>> json2findLoginStream
                = jsonedAgentStream.map(new Json2FindScan())
                //过滤掉非json格式的数据
                .filter(a -> a != null);

        SingleOutputStreamOperator<AssetFindScan> processAssetFindLonigStream = json2findLoginStream.process(new ProcessFunction<List<AssetFindScan>, AssetFindScan>() {
            @Override
            public void processElement(List<AssetFindScan> findLogins, Context context, Collector<AssetFindScan> out) throws Exception {
                for (AssetFindScan assetFindLogin : findLogins) {
                    out.collect(assetFindLogin);
                }
            }
        });

        //processAssetFindLonigStream.print();
        //开窗批量插入
        processAssetFindLonigStream.timeWindowAll(Time.milliseconds(timeout))
                .apply(new AllWindowFunction<AssetFindScan, List<AssetFindScan>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<AssetFindScan> values, Collector<List<AssetFindScan>> out) throws Exception {
                        ArrayList<AssetFindScan> objectNodes = Lists.newArrayList(values);
                        if (objectNodes.size() > 0) {
                            out.collect(objectNodes);
                        }
                    }
                }).addSink(new AssetDiscoverScanMysqlSink(producerBrokers, producerTopic, jdbcUrl, userName, password));

        env.execute("(scan)Asset discover job (java).");
    }


    /**
     * 将Agent原始json字符串转成AgentCollect Pojo
     */
    public static class Json2FindScan implements MapFunction<ObjectNode, List<AssetFindScan>>{
        private static Logger LOG = Logger.getLogger(Json2FindScan.class);
        @Override
        public List<AssetFindScan> map(ObjectNode o0) throws Exception {

            try {

                JSONObject o = JSON.parseObject(o0.toString());
                ArrayList<AssetFindScan> list = new ArrayList<>();
                String resourceName = o.get(AssetFindConstant.RESOURCE_NAME).toString();
                String taskId = o.get(AssetFindConstant.TASK_ID).toString();
                String scanTime = o.get(AssetFindConstant.SCAN_TIME).toString();
                String ipsStr = o.getString(AssetFindConstant.RESOURCE_INFO);
                List<String> ips = JSON.parseArray(ipsStr, String.class);

                AssetFindScan assetFindScan = new AssetFindScan();
                assetFindScan.setResourceName(resourceName);
                assetFindScan.setTaskID(taskId);
                assetFindScan.setScanTime(scanTime);
                assetFindScan.setIps(ips);

                list.add(assetFindScan);
                return list;
            }catch (Exception e){
                LOG.error(e.getMessage(), e);
                return null;
            }
        }
    }

}
