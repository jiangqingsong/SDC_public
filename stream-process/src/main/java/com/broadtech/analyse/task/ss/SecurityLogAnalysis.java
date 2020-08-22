package com.broadtech.analyse.task.ss;

import com.broadtech.analyse.flink.bf.SecurityLogWithBloom;
import com.broadtech.analyse.flink.deserialization.CustomJSONDeserializationSchema;
import com.broadtech.analyse.flink.function.ss.SecurityJson2SecurityLog;
import com.broadtech.analyse.flink.process.ss.ComputeSecurityEvenCountFunc;
import com.broadtech.analyse.flink.process.ss.IntelligenceProcessFunc;
import com.broadtech.analyse.flink.sink.ss.EventAlarm2MysqlSink;
import com.broadtech.analyse.flink.source.ss.IntelligenceLibReader;
import com.broadtech.analyse.pojo.ss.SecurityLog;
import com.broadtech.analyse.pojo.ss.ThreatIntelligence2;
import com.broadtech.analyse.pojo.ss.ThreatIntelligenceMaps;
import com.broadtech.analyse.util.TimeUtils;
import com.broadtech.analyse.util.env.FlinkUtils;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jiangqingsong
 * @description 安全日志分析
 * @date 2020-08-03 17:28
 */
public class SecurityLogAnalysis {

    private static Logger LOG = Logger.getLogger(SecurityLogAnalysis.class);
    private static final OutputTag<ObjectNode> jsonFormatTag = new OutputTag<ObjectNode>("JsonFormat") {
    };
    private static final String TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public static void main(String[] args) throws Exception {

        String propPath = "D:\\SDC\\gitlab_code\\sdcplatform\\SDCPlatform\\stream-process\\src\\main\\resources\\ss_security_analysis.properties";
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //String propPath = parameterTool.get("conf_path");
        //获取配置参数
        ParameterTool paramFromProps = ParameterTool.fromPropertiesFile(propPath);
        String consumerTopic = paramFromProps.get("consumer.topic");
        String producerBrokers = paramFromProps.get("producer.bootstrap.server");
        String groupId = paramFromProps.get("consumer.groupId");

        Long timeout = paramFromProps.getLong("timeout");
        Long windowSize = paramFromProps.getLong("windowSize");
        Long batchSize = paramFromProps.getLong("batchSize");

        //sink mysql config
        String jdbcUrl = paramFromProps.get("jdbcUrl");
        String userName = paramFromProps.get("userName");
        String password = paramFromProps.get("password");

        //env
        final StreamExecutionEnvironment env = FlinkUtils.getEnv();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        //env.registerType(ThreatIntelligenceMaps.class);

        DataStream<ObjectNode> kafkaSourceStream = FlinkUtils.createKafkaStream(true, paramFromProps, consumerTopic, groupId, CustomJSONDeserializationSchema.class);
        DataStream<ObjectNode> filteredKafkaSourceStream = kafkaSourceStream.process(new ProcessFunction<ObjectNode, ObjectNode>() {
            @Override
            public void processElement(ObjectNode value, Context ctx, Collector<ObjectNode> out) throws Exception {
                if (!value.has("jsonParseError")) {
                    ctx.output(jsonFormatTag, value);
                }
            }
        }).getSideOutput(jsonFormatTag);

        //json2security
        SingleOutputStreamOperator<SecurityLog> securityObjStream = filteredKafkaSourceStream.map(new SecurityJson2SecurityLog())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SecurityLog>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(SecurityLog securityLog) {
                        return TimeUtils.getTimestamp(TIME_PATTERN, securityLog.getEventgeneratetime());
                    }
                });
        //distinct
        SingleOutputStreamOperator<SecurityLog> securityObjWithBloomStream = securityObjStream.process(new SecurityLogWithBloom());
        //开窗计数
        KeyedStream<SecurityLog, Integer> securityLogIntegerKeyedStream = securityObjWithBloomStream.keyBy(x -> 0);

        SingleOutputStreamOperator<Tuple3<SecurityLog, Integer, Long>> securityWithCountStream = securityLogIntegerKeyedStream
                .timeWindow(Time.seconds(windowSize))
                .process(new ComputeSecurityEvenCountFunc());

        //3、关联分析 - 威胁情报
        //4、漏洞库
        //配置流&&广播流
        DataStreamSource<ThreatIntelligenceMaps> intelligenceStream = env.addSource(new IntelligenceLibReader(jdbcUrl, userName, password));
        final MapStateDescriptor<Void, ThreatIntelligenceMaps> configDescriptor = new MapStateDescriptor(
                "threatIntelligenceMap", Types.VOID,
                Types.GENERIC(ThreatIntelligenceMaps.class));

        BroadcastStream<ThreatIntelligenceMaps> broadcastStream = intelligenceStream.broadcast(configDescriptor);

        //主流数据和广播流数据connect
        SingleOutputStreamOperator<Tuple5<SecurityLog, Integer, Long, String, ThreatIntelligence2>> intelligenceProcessedStream
                = securityWithCountStream.connect(broadcastStream).process(new IntelligenceProcessFunc(jdbcUrl, userName, password));

        //sink
        SingleOutputStreamOperator<List<Tuple5<SecurityLog, Integer, Long, String, ThreatIntelligence2>>> addBatchIntelligenceStream = intelligenceProcessedStream
                .countWindowAll(batchSize)
                .apply(new AllWindowFunction<Tuple5<SecurityLog, Integer, Long, String, ThreatIntelligence2>, List<Tuple5<SecurityLog, Integer, Long, String, ThreatIntelligence2>>, GlobalWindow>() {

                    @Override
                    public void apply(GlobalWindow window, Iterable<Tuple5<SecurityLog, Integer, Long, String, ThreatIntelligence2>> values, Collector<List<Tuple5<SecurityLog, Integer, Long, String, ThreatIntelligence2>>> out) throws Exception {
                        ArrayList objectNodes = Lists.newArrayList(values);
                        if (objectNodes.size() > 0) {
                            out.collect(objectNodes);
                        }
                    }
                });

        addBatchIntelligenceStream.addSink(new EventAlarm2MysqlSink(jdbcUrl, userName, password));

        env.execute("SecurityLog Alarm Analysis Job.");
    }
}
